use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::sync::mpsc::Receiver;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum Value {
    None,
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(String),
    List(Rc<RefCell<Vec<Value>>>),
    Tuple(Vec<Value>),
    Dict(Rc<RefCell<HashMap<HashKey, Value>>>),
    Set(Rc<RefCell<HashSet<HashKey>>>),
    Function(FunctionValue),
    Class(ClassValue),
    Instance(InstanceValue),
    BoundMethod(Box<BoundMethodValue>),
    File(FileValue),
    Generator(GeneratorValue),
}

#[allow(dead_code)]
impl Value {
    pub fn truthy(&self) -> bool {
        match self {
            Value::None => false,
            Value::Bool(b) => *b,
            Value::Int(i) => *i != 0,
            Value::Float(f) => *f != 0.0,
            Value::Str(s) => !s.is_empty(),
            Value::List(items) => !items.borrow().is_empty(),
            Value::Tuple(items) => !items.is_empty(),
            Value::Dict(map) => !map.borrow().is_empty(),
            Value::Set(set) => !set.borrow().is_empty(),
            Value::Function(_)
            | Value::Class(_)
            | Value::Instance(_)
            | Value::BoundMethod(_)
            | Value::File(_)
            | Value::Generator(_) => true,
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Value::None => "none",
            Value::Int(_) => "int",
            Value::Float(_) => "float",
            Value::Bool(_) => "bool",
            Value::Str(_) => "str",
            Value::List(_) => "list",
            Value::Tuple(_) => "tuple",
            Value::Dict(_) => "dict",
            Value::Set(_) => "set",
            Value::Function(_) => "function",
            Value::Class(_) => "class",
            Value::Instance(_) => "instance",
            Value::BoundMethod(_) => "bound_method",
            Value::File(_) => "file",
            Value::Generator(_) => "generator",
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct FunctionValue {
    pub name: Option<String>,
    pub arity: usize,
    pub impls: FunctionImpl,
}

#[allow(dead_code)]
#[derive(Clone)]
pub enum FunctionImpl {
    Native(NativeFunc),
    User(usize),
}

impl fmt::Debug for FunctionImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionImpl::Native(_) => f.write_str("Native"),
            FunctionImpl::User(id) => write!(f, "User({id})"),
        }
    }
}

pub type NativeFunc = Rc<
    dyn Fn(
        &[Value],
        &[(String, Value)],
        &mut Env,
        &mut HashMap<String, Value>,
    ) -> Result<Value, RuntimeError>,
>;

#[derive(Debug, Clone)]
pub struct Env {
    pub scopes: Vec<HashMap<String, Value>>,
}

impl Env {
    pub fn new() -> Self {
        Env {
            scopes: vec![HashMap::new()],
        }
    }

    pub fn child(&self) -> Self {
        let mut scopes = self.scopes.clone();
        scopes.push(HashMap::new());
        Env { scopes }
    }

    pub fn get(&self, name: &str) -> Option<Value> {
        for scope in self.scopes.iter().rev() {
            if let Some(v) = scope.get(name) {
                return Some(v.clone());
            }
        }
        None
    }

    pub fn set(&mut self, name: &str, value: Value) {
        if let Some(scope) = self.scopes.last_mut() {
            scope.insert(name.to_string(), value);
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ClassValue {
    pub name: String,
    pub methods: HashMap<String, FunctionValue>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct InstanceValue {
    pub class: Rc<ClassValue>,
    pub fields: Rc<RefCell<HashMap<String, Value>>>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct BoundMethodValue {
    pub receiver: Value,
    pub func: FunctionValue,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct FileValue {
    pub path: String,
    pub mode: String,
    pub file: Rc<RefCell<File>>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct GeneratorValue {
    pub rx: Rc<Receiver<Result<Value, RuntimeError>>>,
}

// Simple whence constants to mirror std::io::SeekFrom needs.
pub const SEEK_SET: i64 = 0;
pub const SEEK_CUR: i64 = 1;
pub const SEEK_END: i64 = 2;

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HashKey {
    Int(i64),
    FloatOrdered(i64),
    Bool(bool),
    Str(String),
}

impl Hash for HashKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            HashKey::Int(v) => {
                0u8.hash(state);
                v.hash(state);
            }
            HashKey::FloatOrdered(bits) => {
                1u8.hash(state);
                bits.hash(state);
            }
            HashKey::Bool(v) => {
                2u8.hash(state);
                v.hash(state);
            }
            HashKey::Str(s) => {
                3u8.hash(state);
                s.hash(state);
            }
        }
    }
}

impl HashKey {
    pub fn from_value(v: &Value) -> Option<HashKey> {
        match v {
            Value::Int(i) => Some(HashKey::Int(*i)),
            Value::Float(f) => Some(HashKey::FloatOrdered(f.to_bits() as i64)),
            Value::Bool(b) => Some(HashKey::Bool(*b)),
            Value::Str(s) => Some(HashKey::Str(s.clone())),
            _ => None,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct RuntimeError {
    pub message: String,
}

impl RuntimeError {
    pub fn new(msg: impl Into<String>) -> Self {
        RuntimeError {
            message: msg.into(),
        }
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Runtime error: {}", self.message)
    }
}

impl std::error::Error for RuntimeError {}

pub mod http {
    use super::{
        ClassValue, FunctionImpl, FunctionValue, HashKey, InstanceValue, RuntimeError, Value,
    };
    use crate::interpreter::value_to_string;
    use reqwest::blocking::{multipart, Client};
    use reqwest::redirect;
    use reqwest::{header::HeaderName, header::HeaderValue, Method, Url};
    use std::sync::{Arc, Mutex};
    use serde_json::Value as JsonValue;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::env;
    use std::fs;
    use std::net::IpAddr;
    use std::rc::Rc;
    use std::time::{Duration, Instant, SystemTime};
    use ipnet::IpNet;

    const DEFAULT_REDIRECTS: usize = 10;
    const DEFAULT_TIMEOUT_SECS: f64 = 30.0;

    #[derive(Clone)]
    struct CookieEntry {
        name: String,
        value: String,
        domain: Option<String>,
        path: Option<String>,
        secure: bool,
        httponly: bool,
        expires_at: Option<f64>,
        same_site: Option<String>,
        host: Option<String>,
        host_only: bool,
        persistent: bool,
    }

    pub fn build_requests_module() -> Value {
        let mut fields = HashMap::new();
        fields.insert(
            "request".into(),
            make_module_function("request", move |args, kwargs, _env, _globals| {
                let method = match args.get(0) {
                    Some(Value::Str(s)) => s.clone(),
                    _ => return Err(RuntimeError::new("request expects method string")),
                };
                let url = args.get(1).ok_or_else(|| RuntimeError::new("request expects url"))?;
                perform_request(method, url.clone(), kwargs, None)
            }),
        );

        for (name, verb) in [
            ("get", "GET"),
            ("post", "POST"),
            ("put", "PUT"),
            ("delete", "DELETE"),
            ("patch", "PATCH"),
            ("head", "HEAD"),
            ("options", "OPTIONS"),
        ] {
            fields.insert(
                name.into(),
                make_module_function(name, move |args, kwargs, _env, _globals| {
                    let url = args.get(0).ok_or_else(|| RuntimeError::new("missing url"))?;
                    perform_request(verb.to_string(), url.clone(), kwargs, None)
                }),
            );
        }

        fields.insert("Session".into(), Value::Class(session_class()));
        let module = InstanceValue {
            class: Rc::new(ClassValue {
                name: "requests".into(),
                methods: HashMap::new(),
            }),
            fields: Rc::new(RefCell::new(fields)),
        };
        module
            .fields
            .borrow_mut()
            .insert("codes".into(), status_codes_value());
        module
            .fields
            .borrow_mut()
            .insert("exceptions".into(), exceptions_value());
        Value::Instance(module)
    }

    fn make_module_function(
        name: &str,
        func: impl Fn(&[Value], &[(String, Value)], &mut super::Env, &mut HashMap<String, Value>) -> Result<Value, RuntimeError>
            + 'static,
    ) -> Value {
        Value::Function(FunctionValue {
            name: Some(name.into()),
            arity: 0,
            impls: FunctionImpl::Native(Rc::new(func)),
        })
    }

    fn status_codes_value() -> Value {
        let mut map = HashMap::new();
        for (code, text) in [
            (200, "ok"),
            (201, "created"),
            (204, "no_content"),
            (301, "moved_permanently"),
            (302, "found"),
            (400, "bad_request"),
            (401, "unauthorized"),
            (403, "forbidden"),
            (404, "not_found"),
            (408, "request_timeout"),
            (429, "too_many_requests"),
            (500, "internal_server_error"),
            (502, "bad_gateway"),
            (503, "service_unavailable"),
        ] {
            map.insert(HashKey::Int(code), Value::Str(text.into()));
        }
        Value::Dict(Rc::new(RefCell::new(map)))
    }

    fn exceptions_value() -> Value {
        let mut map = HashMap::new();
        for name in [
            "RequestException",
            "HTTPError",
            "ConnectionError",
            "Timeout",
            "TooManyRedirects",
            "InvalidURL",
            "SSLError",
        ] {
            map.insert(HashKey::Str(name.into()), Value::Str(name.into()));
        }
        Value::Dict(Rc::new(RefCell::new(map)))
    }

    fn session_class() -> ClassValue {
        let mut methods = HashMap::new();

        methods.insert(
            "__init__".into(),
            FunctionValue {
                name: Some("__init__".into()),
                arity: 1,
                impls: FunctionImpl::Native(Rc::new(|args, _kwargs, _env, _globals| {
                    let inst = match args.get(0) {
                        Some(Value::Instance(i)) => i,
                        _ => return Err(RuntimeError::new("invalid Session instance")),
                    };
                    let mut fields = inst.fields.borrow_mut();
                    fields.insert("headers".into(), empty_dict());
                    fields.insert("params".into(), empty_dict());
                    fields.insert("cookies".into(), empty_dict());
                    fields.insert("auth".into(), Value::None);
                    fields.insert("proxies".into(), empty_dict());
                    fields.insert("verify".into(), Value::Bool(true));
                    fields.insert("ca_bundle".into(), Value::None);
                    fields.insert("cert".into(), Value::None);
                    fields.insert("allow_redirects".into(), Value::Bool(true));
                    fields.insert("timeout".into(), Value::None);
                    Ok(Value::None)
                })),
            },
        );

        methods.insert(
            "request".into(),
            FunctionValue {
                name: Some("request".into()),
                arity: 2,
                impls: FunctionImpl::Native(Rc::new(
                    |args, kwargs, _env, _globals| -> Result<Value, RuntimeError> {
                        let (inst, method, url) = parse_session_request_args(args)?;
                        perform_request(method, url, kwargs, Some(inst))
                    },
                )),
            },
        );

        for (name, verb) in [
            ("get", "GET"),
            ("post", "POST"),
            ("put", "PUT"),
            ("delete", "DELETE"),
            ("patch", "PATCH"),
            ("head", "HEAD"),
            ("options", "OPTIONS"),
        ] {
            methods.insert(
                name.into(),
                FunctionValue {
                    name: Some(name.into()),
                    arity: 1,
                    impls: FunctionImpl::Native(Rc::new(
                        move |args, kwargs, _env, _globals| -> Result<Value, RuntimeError> {
                            let (inst, url) = parse_session_method_args(args)?;
                            perform_request(verb.to_string(), url, kwargs, Some(inst))
                        },
                    )),
                },
            );
        }

        ClassValue {
            name: "Session".into(),
            methods,
        }
    }

    fn parse_session_request_args(
        args: &[Value],
    ) -> Result<(InstanceValue, String, Value), RuntimeError> {
        let inst = match args.get(0) {
            Some(Value::Instance(i)) => i.clone(),
            _ => return Err(RuntimeError::new("Session.request expects self instance")),
        };
        let method = match args.get(1) {
            Some(Value::Str(s)) => s.clone(),
            _ => return Err(RuntimeError::new("Session.request expects method string")),
        };
        let url = args
            .get(2)
            .ok_or_else(|| RuntimeError::new("Session.request expects url"))?
            .clone();
        Ok((inst, method, url))
    }

    fn parse_session_method_args(args: &[Value]) -> Result<(InstanceValue, Value), RuntimeError> {
        let inst = match args.get(0) {
            Some(Value::Instance(i)) => i.clone(),
            _ => return Err(RuntimeError::new("Session method expects self instance")),
        };
        let url = args
            .get(1)
            .ok_or_else(|| RuntimeError::new("missing url"))?
            .clone();
        Ok((inst, url))
    }

    fn perform_request(
        method: String,
        url: Value,
        kwargs: &[(String, Value)],
        session: Option<InstanceValue>,
    ) -> Result<Value, RuntimeError> {
        let url_str = match url {
            Value::Str(s) => s,
            _ => return Err(RuntimeError::new("url must be string")),
        };
        let parsed_url = parse_url(&url_str)?;

        let mut headers = HashMap::new();
        let mut params = Vec::new();
        let mut cookies = Vec::new();
        let mut auth: Option<(String, String)> = None;
        let mut proxies = HashMap::new();
        let mut verify = true;
        let mut ca_bundle: Option<String> = None;
        let mut cert: Option<(String, Option<String>)> = None;
        let mut allow_redirects = true;
        let mut timeout = default_timeout();
        let no_proxy = collect_no_proxy();

        if let Some(sess) = session.as_ref() {
            let fields = sess.fields.borrow();
            if let Some(Value::Dict(d)) = fields.get("headers") {
                headers.extend(dict_to_string_map(d)?);
            }
            if let Some(Value::Dict(d)) = fields.get("params") {
                params.extend(dict_to_string_pairs(d)?);
            }
            cookies.extend(extract_cookies_from_value(
                fields.get("cookies"),
                Some(&parsed_url),
            )?);
            if let Some(Value::Tuple(items)) = fields.get("auth") {
                if items.len() == 2 {
                    if let (Value::Str(u), Value::Str(p)) = (&items[0], &items[1]) {
                        auth = Some((u.clone(), p.clone()));
                    }
                }
            }
            if let Some(Value::Dict(d)) = fields.get("proxies") {
                proxies.extend(dict_to_string_map(d)?);
            }
            if let Some(Value::Bool(b)) = fields.get("verify") {
                verify = *b;
            }
            if let Some(Value::Str(path)) = fields.get("ca_bundle") {
                ca_bundle = Some(path.clone());
            }
            if let Some(Value::Tuple(items)) = fields.get("cert") {
                if let Some(Value::Str(c)) = items.get(0) {
                    let key = items
                        .get(1)
                        .and_then(|v| match v {
                            Value::Str(s) => Some(s.clone()),
                            _ => None,
                        });
                    cert = Some((c.clone(), key));
                }
            } else if let Some(Value::Str(c)) = fields.get("cert") {
                cert = Some((c.clone(), None));
            }
            if let Some(Value::Bool(b)) = fields.get("allow_redirects") {
                allow_redirects = *b;
            }
            if let Some(v) = fields.get("timeout") {
                if !matches!(v, Value::None) {
                    timeout = Some(value_to_duration(v)?);
                }
            }
        }

        if let Some(h) = get_kw(kwargs, "headers") {
            headers.extend(value_to_string_map(h)?);
        }
        if let Some(p) = get_kw(kwargs, "params") {
            params.extend(value_to_string_pairs(p)?);
        }
        if let Some(c) = get_kw(kwargs, "cookies") {
            cookies.extend(extract_cookies_from_value(Some(c), Some(&parsed_url))?);
        }
        if let Some(a) = get_kw(kwargs, "auth") {
            auth = tuple_to_auth(a)?;
        }
        if let Some(p) = get_kw(kwargs, "proxies") {
            proxies.extend(value_to_string_map(p)?);
        }
        load_env_proxies(&mut proxies);
        if let Some(v) = get_kw(kwargs, "verify") {
            verify = value_to_bool(v)?;
        }
        if let Some(v) = get_kw(kwargs, "ca_bundle") {
            if let Value::Str(s) = v {
                ca_bundle = Some(s.clone());
            }
        }
        if let Some(v) = get_kw(kwargs, "cert") {
            cert = cert_value(v)?;
        }
        if let Some(v) = get_kw(kwargs, "allow_redirects") {
            allow_redirects = value_to_bool(v)?;
        }
        if let Some(v) = get_kw(kwargs, "timeout") {
            timeout = Some(value_to_duration(v)?);
        }

        let data = get_kw(kwargs, "data").cloned();
        let files = get_kw(kwargs, "files").cloned();
        let json_body = if let Some(j) = get_kw(kwargs, "json") {
            Some(value_to_json(j)?)
        } else {
            None
        };

        let history = Arc::new(Mutex::new(Vec::new()));
        let proxy_bypass = should_bypass_proxy(&parsed_url, &no_proxy);
        if proxy_bypass {
            proxies.clear();
        }

        let client = build_client(
            allow_redirects,
            timeout,
            &proxies,
            verify,
            ca_bundle.as_deref(),
            cert,
            history.clone(),
        )?;
        let method = Method::from_bytes(method.as_bytes())
            .map_err(|_| RuntimeError::new("invalid HTTP method"))?;
        let mut request = client
            .request(method.clone(), parsed_url.clone())
            .query(&params);
        for (k, v) in headers {
            let name = HeaderName::from_bytes(k.as_bytes())
                .map_err(|_| RuntimeError::new("invalid header name"))?;
            let value =
                HeaderValue::from_str(&v).map_err(|_| RuntimeError::new("invalid header value"))?;
            request = request.header(name, value);
        }
        if let Some(json) = json_body {
            request = request.json(&json);
        } else if let Some(files_val) = files {
            let form = build_multipart_form(data.as_ref(), &files_val)?;
            request = request.multipart(form);
        } else if let Some(body_val) = data {
            if let Value::Dict(d) = &body_val {
                request = request.form(&dict_to_string_pairs(d)?);
            } else {
                request = request.body(value_to_body(&body_val)?);
            }
        }
        if let Some((user, pass)) = auth {
            request = request.basic_auth(user, Some(pass));
        }
        let cookie_header = cookie_header_for_url(&cookies, parsed_url.clone());
        if let Some(header) = cookie_header {
            request = request.header(reqwest::header::COOKIE, header);
        }

        let start = Instant::now();
        let resp = request
            .send()
            .map_err(|e| RuntimeError::new(format!("request error {} {}: {e}", method, url_str)))?;
        let elapsed = start.elapsed().as_secs_f64();
        let status = resp.status();
        let reason = status.canonical_reason().unwrap_or("").to_string();
        let final_url = resp.url().to_string();
        let headers = resp.headers().clone();
        let resp_cookies = cookies_value(&resp, resp.url());

        if let Some(sess) = session.as_ref() {
            if let Some(Value::Dict(d)) = sess.fields.borrow_mut().get_mut("cookies") {
                merge_cookie_entries(d, &resp_cookies, resp.url())?;
            }
        }

        let body_bytes = resp
            .bytes()
            .map_err(|e| RuntimeError::new(format!("read error: {e}")))?;

        let history_val = history_to_value(&history)?;

        Ok(make_response(
            status.as_u16() as i64,
            reason,
            final_url,
            &headers,
            body_bytes.to_vec(),
            elapsed,
            history_val,
            resp_cookies,
        ))
    }

    fn make_response(
        status: i64,
        reason: String,
        url: String,
        headers: &reqwest::header::HeaderMap,
        body: Vec<u8>,
        elapsed: f64,
        history: Value,
        cookies: Value,
    ) -> Value {
        let class = response_class();
        let mut fields = HashMap::new();
        fields.insert("status_code".into(), Value::Int(status));
        fields.insert("reason".into(), Value::Str(reason));
        fields.insert("url".into(), Value::Str(url));
        fields.insert("ok".into(), Value::Bool(status >= 200 && status < 400));
        fields.insert("elapsed".into(), Value::Float(elapsed));
        fields.insert("content".into(), bytes_to_value(&body));
        fields.insert("text".into(), Value::Str(String::from_utf8_lossy(&body).to_string()));
        fields.insert("headers".into(), headers_to_value(headers));
        fields.insert("history".into(), history);
        fields.insert("cookies".into(), cookies);
        Value::Instance(InstanceValue {
            class,
            fields: Rc::new(RefCell::new(fields)),
        })
    }

    fn response_class() -> Rc<ClassValue> {
        let mut methods = HashMap::new();
        methods.insert(
            "raise_for_status".into(),
            FunctionValue {
                name: Some("raise_for_status".into()),
                arity: 1,
                impls: FunctionImpl::Native(Rc::new(|args, _kwargs, _env, _globals| {
                    let (status, reason) = response_status(args)?;
                    if status >= 400 {
                        return Err(RuntimeError::new(format!(
                            "HTTP error {}: {}",
                            status, reason
                        )));
                    }
                    Ok(Value::None)
                })),
            },
        );

        methods.insert(
            "json".into(),
            FunctionValue {
                name: Some("json".into()),
                arity: 1,
                impls: FunctionImpl::Native(Rc::new(|args, _kwargs, _env, _globals| {
                    let text = match response_field(args, "text")? {
                        Value::Str(s) => s,
                        _ => return Err(RuntimeError::new("response body not string")),
                    };
                    let parsed: JsonValue = serde_json::from_str(&text)
                        .map_err(|e| RuntimeError::new(format!("json decode error: {e}")))?;
                    Ok(json_to_value(&parsed))
                })),
            },
        );

        methods.insert(
            "iter_content".into(),
            FunctionValue {
                name: Some("iter_content".into()),
                arity: 1,
                impls: FunctionImpl::Native(Rc::new(|args, _kwargs, _env, _globals| {
                    let bytes = response_bytes(args)?;
                    let chunk_size = args
                        .get(1)
                        .and_then(|v| match v {
                            Value::Int(i) => Some(*i as usize),
                            Value::Float(f) => Some(*f as usize),
                            _ => None,
                        })
                        .unwrap_or(bytes.len());
                    let chunks = bytes
                        .chunks(chunk_size.max(1))
                        .map(bytes_to_value)
                        .collect::<Vec<_>>();
                    Ok(Value::List(Rc::new(RefCell::new(chunks))))
                })),
            },
        );

        methods.insert(
            "iter_lines".into(),
            FunctionValue {
                name: Some("iter_lines".into()),
                arity: 1,
                impls: FunctionImpl::Native(Rc::new(|args, _kwargs, _env, _globals| {
                    let text = match response_field(args, "text")? {
                        Value::Str(s) => s,
                        _ => return Err(RuntimeError::new("response body not string")),
                    };
                    let lines: Vec<Value> = text
                        .lines()
                        .map(|l| Value::Str(l.to_string()))
                        .collect();
                    Ok(Value::List(Rc::new(RefCell::new(lines))))
                })),
            },
        );

        Rc::new(ClassValue {
            name: "Response".into(),
            methods,
        })
    }

    fn response_field(args: &[Value], field: &str) -> Result<Value, RuntimeError> {
        let inst = match args.get(0) {
            Some(Value::Instance(i)) => i.clone(),
            _ => return Err(RuntimeError::new("method expects response instance")),
        };
        let val = inst
            .fields
            .borrow()
            .get(field)
            .cloned()
            .ok_or_else(|| RuntimeError::new("missing response field"))?;
        Ok(val)
    }

    fn response_status(args: &[Value]) -> Result<(i64, String), RuntimeError> {
        let inst = match args.get(0) {
            Some(Value::Instance(i)) => i.clone(),
            _ => return Err(RuntimeError::new("method expects response instance")),
        };
        let fields = inst.fields.borrow();
        let status = match fields.get("status_code") {
            Some(Value::Int(i)) => *i,
            _ => 0,
        };
        let reason = match fields.get("reason") {
            Some(Value::Str(s)) => s.clone(),
            _ => "".into(),
        };
        Ok((status, reason))
    }

    fn build_client(
        allow_redirects: bool,
        timeout: Option<Duration>,
        proxies: &HashMap<String, String>,
        verify: bool,
        ca_bundle: Option<&str>,
        cert: Option<(String, Option<String>)>,
        history: Arc<Mutex<Vec<String>>>,
    ) -> Result<Client, RuntimeError> {
        let mut builder = Client::builder();
        if allow_redirects {
            let hist = history.clone();
            builder = builder.redirect(redirect::Policy::custom(move |attempt| {
                if let Some(prev) = attempt.previous().last() {
                    if let Ok(mut h) = hist.lock() {
                        h.push(prev.to_string());
                    }
                }
                if attempt.previous().len() >= DEFAULT_REDIRECTS {
                    attempt.error("too many redirects")
                } else {
                    attempt.follow()
                }
            }));
        } else {
            builder = builder.redirect(redirect::Policy::none());
        }
        for (k, v) in proxies {
            let proxy = match k.as_str() {
                "http" => reqwest::Proxy::http(v),
                "https" => reqwest::Proxy::https(v),
                _ => return Err(RuntimeError::new("proxy keys must be http/https")),
            }
            .map_err(|e| RuntimeError::new(format!("proxy error: {e}")))?;
            builder = builder.proxy(proxy);
        }
        if !verify {
            builder = builder.danger_accept_invalid_certs(true);
        }
        if let Some(path) = ca_bundle {
            let pem = fs::read(path).map_err(|e| RuntimeError::new(format!("ca read error: {e}")))?;
            let cert = reqwest::Certificate::from_pem(&pem)
                .map_err(|e| RuntimeError::new(format!("ca parse error: {e}")))?;
            builder = builder.add_root_certificate(cert);
        }
        if let Some((cert_path, key_path)) = cert {
            let mut pem = fs::read(cert_path)
                .map_err(|e| RuntimeError::new(format!("cert read error: {e}")))?;
            if let Some(k) = key_path {
                let key_bytes =
                    fs::read(k).map_err(|e| RuntimeError::new(format!("key read error: {e}")))?;
                pem.extend_from_slice(&key_bytes);
            }
            let id = reqwest::Identity::from_pem(&pem)
                .map_err(|e| RuntimeError::new(format!("identity parse error: {e}")))?;
            builder = builder.identity(id);
        }
        if let Some(t) = timeout {
            builder = builder.timeout(t);
        }
        builder
            .build()
            .map_err(|e| RuntimeError::new(format!("client error: {e}")))
    }

    fn parse_url(url: &str) -> Result<Url, RuntimeError> {
        Url::parse(url).map_err(|e| RuntimeError::new(format!("invalid url: {e}")))
    }

    fn headers_to_value(headers: &reqwest::header::HeaderMap) -> Value {
        let mut map = HashMap::new();
        for (k, v) in headers.iter() {
            let key = HashKey::Str(k.to_string());
            let val = v.to_str().unwrap_or_default().to_string();
            map.insert(key, Value::Str(val));
        }
        Value::Dict(Rc::new(RefCell::new(map)))
    }

    fn empty_dict() -> Value {
        Value::Dict(Rc::new(RefCell::new(HashMap::new())))
    }

    fn get_kw<'a>(kwargs: &'a [(String, Value)], name: &str) -> Option<&'a Value> {
        kwargs.iter().find_map(|(k, v)| if k == name { Some(v) } else { None })
    }

    fn value_to_string_map(v: &Value) -> Result<HashMap<String, String>, RuntimeError> {
        match v {
            Value::Dict(d) => dict_to_string_map(d),
            _ => Err(RuntimeError::new("headers must be dict")),
        }
    }

    fn value_to_string_pairs(v: &Value) -> Result<Vec<(String, String)>, RuntimeError> {
        match v {
            Value::Dict(d) => Ok(dict_to_string_pairs(d)?),
            Value::List(items) => {
                let mut out = Vec::new();
                for item in items.borrow().iter() {
                    match item {
                        Value::Tuple(t) if t.len() == 2 => {
                            let key = value_to_string(&t[0]);
                            let val = value_to_string(&t[1]);
                            out.push((key, val));
                        }
                        _ => {
                            return Err(RuntimeError::new(
                                "params must be dict or list of (k,v) tuples",
                            ))
                        }
                    }
                }
                Ok(out)
            }
            _ => Err(RuntimeError::new("params must be dict or list of tuples")),
        }
    }

    fn dict_to_string_map(
        d: &Rc<RefCell<HashMap<HashKey, Value>>>,
    ) -> Result<HashMap<String, String>, RuntimeError> {
        let mut out = HashMap::new();
        for (k, v) in d.borrow().iter() {
            let key = match k {
                HashKey::Str(s) => s.clone(),
                HashKey::Int(i) => i.to_string(),
                HashKey::FloatOrdered(f) => f64::from_bits(*f as u64).to_string(),
                HashKey::Bool(b) => b.to_string(),
            };
            out.insert(key, value_to_string(v));
        }
        Ok(out)
    }

    fn dict_to_string_pairs(
        d: &Rc<RefCell<HashMap<HashKey, Value>>>,
    ) -> Result<Vec<(String, String)>, RuntimeError> {
        let mut out = Vec::new();
        for (k, v) in d.borrow().iter() {
            let key = match k {
                HashKey::Str(s) => s.clone(),
                HashKey::Int(i) => i.to_string(),
                HashKey::FloatOrdered(f) => f64::from_bits(*f as u64).to_string(),
                HashKey::Bool(b) => b.to_string(),
            };
            out.push((key, value_to_string(v)));
        }
        Ok(out)
    }

    fn value_to_json(v: &Value) -> Result<JsonValue, RuntimeError> {
        match v {
            Value::None => Ok(JsonValue::Null),
            Value::Int(i) => Ok(JsonValue::from(*i)),
            Value::Float(f) => Ok(JsonValue::from(*f)),
            Value::Bool(b) => Ok(JsonValue::from(*b)),
            Value::Str(s) => Ok(JsonValue::from(s.clone())),
            Value::List(items) => {
                let mut arr = Vec::new();
                for item in items.borrow().iter() {
                    arr.push(value_to_json(item)?);
                }
                Ok(JsonValue::Array(arr))
            }
            Value::Tuple(items) => {
                let mut arr = Vec::new();
                for item in items.iter() {
                    arr.push(value_to_json(item)?);
                }
                Ok(JsonValue::Array(arr))
            }
            Value::Dict(map) => {
                let mut obj = serde_json::Map::new();
                for (k, v) in map.borrow().iter() {
                    let key = match k {
                        HashKey::Str(s) => s.clone(),
                        HashKey::Int(i) => i.to_string(),
                        HashKey::FloatOrdered(f) => f64::from_bits(*f as u64).to_string(),
                        HashKey::Bool(b) => b.to_string(),
                    };
                    obj.insert(key, value_to_json(v)?);
                }
                Ok(JsonValue::Object(obj))
            }
            _ => Err(RuntimeError::new("unsupported value for json encoding")),
        }
    }

    fn json_to_value(v: &JsonValue) -> Value {
        match v {
            JsonValue::Null => Value::None,
            JsonValue::Bool(b) => Value::Bool(*b),
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Int(i)
                } else if let Some(f) = n.as_f64() {
                    Value::Float(f)
                } else {
                    Value::Float(0.0)
                }
            }
            JsonValue::String(s) => Value::Str(s.clone()),
            JsonValue::Array(arr) => Value::List(Rc::new(RefCell::new(
                arr.iter().map(json_to_value).collect(),
            ))),
            JsonValue::Object(map) => {
                let mut out = HashMap::new();
                for (k, v) in map.iter() {
                    out.insert(HashKey::Str(k.clone()), json_to_value(v));
                }
                Value::Dict(Rc::new(RefCell::new(out)))
            }
        }
    }

    fn value_to_duration(v: &Value) -> Result<Duration, RuntimeError> {
        match v {
            Value::Int(i) if *i >= 0 => Ok(Duration::from_secs(*i as u64)),
            Value::Float(f) if *f >= 0.0 => Ok(Duration::from_secs_f64(*f)),
            _ => Err(RuntimeError::new("timeout must be non-negative number")),
        }
    }

    fn value_to_bool(v: &Value) -> Result<bool, RuntimeError> {
        match v {
            Value::Bool(b) => Ok(*b),
            _ => Err(RuntimeError::new("expected bool")),
        }
    }

    fn value_to_body(v: &Value) -> Result<reqwest::blocking::Body, RuntimeError> {
        match v {
            Value::Str(s) => Ok(reqwest::blocking::Body::from(s.clone())),
            Value::List(items) => Ok(reqwest::blocking::Body::from(values_to_bytes(items)?)),
            Value::Dict(_) => Err(RuntimeError::new("cannot directly body a dict")),
            _ => Ok(reqwest::blocking::Body::from(value_to_string(v))),
        }
    }

    fn values_to_bytes(items: &Rc<RefCell<Vec<Value>>>) -> Result<Vec<u8>, RuntimeError> {
        let mut out = Vec::new();
        for v in items.borrow().iter() {
            match v {
                Value::Int(i) => out.push(*i as u8),
                _ => return Err(RuntimeError::new("byte list must contain ints")),
            }
        }
        Ok(out)
    }

    fn bytes_to_value(bytes: &[u8]) -> Value {
        Value::List(Rc::new(RefCell::new(
            bytes.iter().map(|b| Value::Int(*b as i64)).collect(),
        )))
    }

    fn response_bytes(args: &[Value]) -> Result<Vec<u8>, RuntimeError> {
        let body = response_field(args, "content")?;
        match body {
            Value::List(list) => values_to_bytes(&list),
            Value::Str(s) => Ok(s.into_bytes()),
            _ => Err(RuntimeError::new("response body not bytes")),
        }
    }

    fn history_to_value(history: &Arc<Mutex<Vec<String>>>) -> Result<Value, RuntimeError> {
        let list = history
            .lock()
            .map_err(|_| RuntimeError::new("redirect history poisoned"))?
            .clone();
        let vals = list.into_iter().map(Value::Str).collect::<Vec<_>>();
        Ok(Value::List(Rc::new(RefCell::new(vals))))
    }

    fn tuple_to_auth(v: &Value) -> Result<Option<(String, String)>, RuntimeError> {
        match v {
            Value::Tuple(items) if items.len() == 2 => {
                if let (Value::Str(u), Value::Str(p)) = (&items[0], &items[1]) {
                    Ok(Some((u.clone(), p.clone())))
                } else {
                    Err(RuntimeError::new("auth tuple must be (user, pass)"))
                }
            }
            Value::None => Ok(None),
            _ => Err(RuntimeError::new("auth must be tuple or None")),
        }
    }

    fn cert_value(v: &Value) -> Result<Option<(String, Option<String>)>, RuntimeError> {
        match v {
            Value::Str(p) => Ok(Some((p.clone(), None))),
            Value::Tuple(items) if !items.is_empty() => {
                let cert_path = match &items[0] {
                    Value::Str(s) => s.clone(),
                    _ => return Err(RuntimeError::new("cert path must be string")),
                };
                let key_path = items
                    .get(1)
                    .and_then(|k| if let Value::Str(s) = k { Some(s.clone()) } else { None });
                Ok(Some((cert_path, key_path)))
            }
            Value::None => Ok(None),
            _ => Err(RuntimeError::new("cert must be path string or (cert, key) tuple")),
        }
    }

    fn extract_cookies_from_value(
        val: Option<&Value>,
        url: Option<&Url>,
    ) -> Result<Vec<CookieEntry>, RuntimeError> {
        let mut out = Vec::new();
        let host = url.and_then(|u| u.host_str().map(|s| s.to_string()));
        let path = url.map(|u| u.path().to_string());
        match val {
            Some(Value::Dict(d)) => {
                for (k, v) in d.borrow().iter() {
                    let name = hashkey_to_string(k);
                    match v {
                        Value::Str(s) => out.push(CookieEntry {
                            name,
                            value: s.clone(),
                            domain: None,
                            path: None,
                            secure: false,
                            httponly: false,
                            expires_at: None,
                            same_site: None,
                            host: url.and_then(|u| u.host_str().map(|h| h.to_string())),
                            host_only: true,
                            persistent: false,
                        }),
                        Value::Dict(inner) => {
                            let value = inner
                                .borrow()
                                .get(&HashKey::Str("value".into()))
                                .and_then(|v| match v {
                                    Value::Str(s) => Some(s.clone()),
                                    _ => None,
                                })
                                .ok_or_else(|| RuntimeError::new("cookie missing value"))?;
                            let domain = inner
                                .borrow()
                                .get(&HashKey::Str("domain".into()))
                                .and_then(|v| match v {
                                    Value::Str(s) => Some(s.clone()),
                                    _ => None,
                                });
                            let path_val = inner
                                .borrow()
                                .get(&HashKey::Str("path".into()))
                                .and_then(|v| match v {
                                    Value::Str(s) => Some(s.clone()),
                                    _ => None,
                                });
                            let secure = inner
                                .borrow()
                                .get(&HashKey::Str("secure".into()))
                                .and_then(|v| match v {
                                    Value::Bool(b) => Some(*b),
                                    _ => None,
                                })
                                .unwrap_or(false);
                            let httponly = inner
                                .borrow()
                                .get(&HashKey::Str("httponly".into()))
                                .and_then(|v| match v {
                                    Value::Bool(b) => Some(*b),
                                    _ => None,
                                })
                                .unwrap_or(false);
                            let expires_at = inner
                                .borrow()
                                .get(&HashKey::Str("expires".into()))
                                .and_then(|v| match v {
                                    Value::Float(f) => Some(*f),
                                    Value::Int(i) => Some(*i as f64),
                                    _ => None,
                                });
                            let max_age = inner
                                .borrow()
                                .get(&HashKey::Str("max_age".into()))
                                .and_then(|v| match v {
                                    Value::Float(f) => Some(*f),
                                    Value::Int(i) => Some(*i as f64),
                                    _ => None,
                                });
                            let same_site = inner
                                .borrow()
                                .get(&HashKey::Str("same_site".into()))
                                .and_then(|v| match v {
                                    Value::Str(s) => Some(s.clone()),
                                    _ => None,
                                });
                            let host_only = inner
                                .borrow()
                                .get(&HashKey::Str("host_only".into()))
                                .and_then(|v| match v {
                                    Value::Bool(b) => Some(*b),
                                    _ => None,
                                })
                                .unwrap_or(domain.is_none());
                            let host_val = inner
                                .borrow()
                                .get(&HashKey::Str("host".into()))
                                .and_then(|v| match v {
                                    Value::Str(s) => Some(s.clone()),
                                    _ => None,
                                })
                                .or_else(|| url.and_then(|u| u.host_str().map(|h| h.to_string())));
                            out.push(CookieEntry {
                                name,
                                value,
                                domain,
                                path: path_val,
                                secure,
                                httponly,
                                expires_at: expires_at.or_else(|| max_age.map(|m| current_timestamp() + m)),
                                same_site,
                                host: host_val,
                                host_only,
                                persistent: expires_at.is_some() || max_age.is_some(),
                            });
                        }
                        _ => {
                            return Err(RuntimeError::new(
                                "cookie entries must be string or dict with value/domain/path",
                            ))
                        }
                    }
                }
            }
            Some(Value::List(list)) => {
                for v in list.borrow().iter() {
                    if let Value::Tuple(items) = v {
                        if items.len() >= 2 {
                            if let (Value::Str(name), Value::Str(value)) = (&items[0], &items[1]) {
                                let domain = items.get(2).and_then(|v| match v {
                                    Value::Str(s) => Some(s.clone()),
                                    _ => None,
                                });
                                let path_val = items.get(3).and_then(|v| match v {
                                    Value::Str(s) => Some(s.clone()),
                                    _ => None,
                                });
                                let host_only = domain.is_none();
                                let secure = items.get(4).and_then(|v| match v {
                                    Value::Bool(b) => Some(*b),
                                    _ => None,
                                }).unwrap_or(false);
                                let httponly = items.get(5).and_then(|v| match v {
                                    Value::Bool(b) => Some(*b),
                                    _ => None,
                                }).unwrap_or(false);
                                let expires = items.get(6).and_then(|v| match v {
                                    Value::Float(f) => Some(*f),
                                    Value::Int(i) => Some(*i as f64),
                                    _ => None,
                                });
                                let same_site = items.get(7).and_then(|v| match v {
                                    Value::Str(s) => Some(s.clone()),
                                    _ => None,
                                });
                                out.push(CookieEntry {
                                    name: name.clone(),
                                    value: value.clone(),
                                    domain,
                                    path: path_val,
                                    secure,
                                    httponly,
                                    expires_at: expires
                                        .or_else(|| {
                                            items.get(8).and_then(|v| match v {
                                                Value::Float(f) => Some(current_timestamp() + *f),
                                                Value::Int(i) => Some(current_timestamp() + *i as f64),
                                                _ => None,
                                            })
                                        }),
                                    same_site,
                                    host: url.and_then(|u| u.host_str().map(|h| h.to_string())),
                                    host_only,
                                    persistent: expires.is_some(),
                                });
                            }
                        }
                    }
                }
            }
            Some(other) => {
                return Err(RuntimeError::new(format!(
                    "unsupported cookies value {:?}",
                    other
                )))
            }
            None => {}
        }
        let now = current_timestamp();
        let filtered = if let (Some(h), Some(p)) = (host, path) {
            out.into_iter()
                .filter(|c| cookie_matches(&h, &p, c, now))
                .collect()
        } else {
            out.into_iter()
                .filter(|c| cookie_not_expired(c, now))
                .collect()
        };
        Ok(filtered)
    }

    fn cookie_not_expired(cookie: &CookieEntry, now: f64) -> bool {
        match cookie.expires_at {
            Some(exp) => now < exp,
            None => true,
        }
    }

    fn cookie_matches(host: &str, path: &str, cookie: &CookieEntry, now: f64) -> bool {
        if !cookie_not_expired(cookie, now) {
            return false;
        }
        let domain_ok = match &cookie.domain {
            Some(d) => {
                let d_trim = d.trim_start_matches('.');
                host == d_trim || host.ends_with(&format!(".{d_trim}"))
            }
            None => {
                if cookie.host_only {
                    cookie.host.as_deref() == Some(host)
                } else {
                    true
                }
            }
        };
        let cookie_path = cookie.path.clone().unwrap_or_else(|| "/".into());
        let path_ok = path.starts_with(&cookie_path);
        domain_ok && path_ok
    }

    fn cookie_header_for_url(cookies: &[CookieEntry], url: Url) -> Option<String> {
        let host = url.host_str()?;
        let path = url.path();
        let now = current_timestamp();
        let parts: Vec<String> = cookies
            .iter()
            .filter(|c| {
                cookie_matches(host, path, c, now) && (!c.secure || url.scheme() == "https")
            })
            .map(|c| format!("{}={}", c.name, c.value))
            .collect();
        if parts.is_empty() {
            None
        } else {
            Some(parts.join("; "))
        }
    }

    fn current_timestamp() -> f64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0)
    }

    fn build_multipart_form(
        data: Option<&Value>,
        files: &Value,
    ) -> Result<multipart::Form, RuntimeError> {
        let mut form = multipart::Form::new();
        if let Some(Value::Dict(d)) = data {
            for (k, v) in d.borrow().iter() {
                form = form.text(hashkey_to_string(k), value_to_string(v));
            }
        }
        match files {
            Value::Dict(d) => {
                for (k, v) in d.borrow().iter() {
                    let name = hashkey_to_string(k);
                    let (bytes, filename) = value_to_file_part(v)?;
                    let mut part = multipart::Part::bytes(bytes);
                    if let Some(fname) = filename {
                        part = part.file_name(fname);
                    }
                    form = form.part(name, part);
                }
            }
            _ => return Err(RuntimeError::new("files must be dict")),
        }
        Ok(form)
    }

    fn hashkey_to_string(k: &HashKey) -> String {
        match k {
            HashKey::Str(s) => s.clone(),
            HashKey::Int(i) => i.to_string(),
            HashKey::FloatOrdered(f) => f64::from_bits(*f as u64).to_string(),
            HashKey::Bool(b) => b.to_string(),
        }
    }

    fn value_to_file_part(v: &Value) -> Result<(Vec<u8>, Option<String>), RuntimeError> {
        match v {
            Value::Str(s) => Ok((s.as_bytes().to_vec(), None)),
            Value::List(items) => Ok((values_to_bytes(items)?, None)),
            Value::Tuple(items) if items.len() == 2 => {
                let filename = match &items[0] {
                    Value::Str(s) => s.clone(),
                    _ => return Err(RuntimeError::new("file tuple must be (name, content)")),
                };
                let bytes = match &items[1] {
                    Value::Str(s) => s.as_bytes().to_vec(),
                    Value::List(items) => values_to_bytes(items)?,
                    Value::File(fv) => fs::read(&fv.path)
                        .map_err(|e| RuntimeError::new(format!("file read error: {e}")))?,
                    _ => return Err(RuntimeError::new("file content must be str/bytes/file")),
                };
                Ok((bytes, Some(filename)))
            }
            Value::File(fv) => Ok((
                fs::read(&fv.path).map_err(|e| RuntimeError::new(format!("file read error: {e}")))?,
                Some(fv.path.clone()),
            )),
            _ => Err(RuntimeError::new("unsupported file part value")),
        }
    }

    fn load_env_proxies(proxies: &mut HashMap<String, String>) {
        for key in ["http_proxy", "HTTP_PROXY"] {
            if !proxies.contains_key("http") {
                if let Ok(val) = env::var(key) {
                    proxies.insert("http".into(), val);
                }
            }
        }
        for key in ["https_proxy", "HTTPS_PROXY"] {
            if !proxies.contains_key("https") {
                if let Ok(val) = env::var(key) {
                    proxies.insert("https".into(), val);
                }
            }
        }
    }

    fn collect_no_proxy() -> Vec<String> {
        let mut entries = Vec::new();
        for key in ["NO_PROXY", "no_proxy"] {
            if let Ok(val) = env::var(key) {
                entries.extend(
                    val.split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty()),
                );
            }
        }
        entries
    }

    fn should_bypass_proxy(url: &Url, no_proxy: &[String]) -> bool {
        let host = match url.host_str() {
            Some(h) => h,
            None => return false,
        };
        let host_ip = host.parse::<IpAddr>().ok();
        for entry in no_proxy {
            let e = entry.trim();
            if e.is_empty() {
                continue;
            }
            if e == "*" {
                return true;
            }
            let entry_host = strip_port(e);
            if entry_host.contains('/') {
                if let Ok(net) = entry_host.parse::<IpNet>() {
                    if let Some(ip) = host_ip {
                        if net.contains(&ip) {
                            return true;
                        }
                    }
                }
                continue;
            }
            if let Some(ip) = host_ip {
                if let Ok(ip_match) = entry_host.parse::<IpAddr>() {
                    if ip == ip_match {
                        return true;
                    }
                }
            }
            if entry_host.starts_with('.') {
                let e = &entry_host[1..];
                if host.ends_with(e) || host == e {
                    return true;
                }
            } else if host == entry_host || host.ends_with(&format!(".{entry_host}")) {
                return true;
            }
        }
        false
    }

    fn strip_port(entry: &str) -> String {
        if let Some(start) = entry.find('[') {
            if let Some(end) = entry[start + 1..].find(']') {
                return entry[start + 1..start + 1 + end].to_string();
            }
        }
        entry
            .split(':')
            .next()
            .map(|s| s.to_string())
            .unwrap_or_else(|| entry.to_string())
    }

    fn default_timeout() -> Option<Duration> {
        if let Ok(raw) = env::var("TYRION_REQUESTS_TIMEOUT") {
            if let Ok(v) = raw.parse::<f64>() {
                if v >= 0.0 {
                    return Some(Duration::from_secs_f64(v));
                }
            }
        }
        Some(Duration::from_secs_f64(DEFAULT_TIMEOUT_SECS))
    }

    fn cookies_value(resp: &reqwest::blocking::Response, url: &Url) -> Value {
        let mut map = HashMap::new();
        let now = current_timestamp();
        for c in resp.cookies() {
            let mut entry = HashMap::new();
            entry.insert(HashKey::Str("value".into()), Value::Str(c.value().to_string()));
            if let Some(domain) = c.domain() {
                entry.insert(HashKey::Str("domain".into()), Value::Str(domain.to_string()));
            }
            if let Some(path) = c.path() {
                entry.insert(HashKey::Str("path".into()), Value::Str(path.to_string()));
            }
            entry.insert(
                HashKey::Str("host".into()),
                Value::Str(url.host_str().unwrap_or_default().to_string()),
            );
            entry.insert(
                HashKey::Str("host_only".into()),
                Value::Bool(c.domain().is_none()),
            );
            if c.http_only() {
                entry.insert(HashKey::Str("httponly".into()), Value::Bool(true));
            }
            if c.secure() {
                entry.insert(HashKey::Str("secure".into()), Value::Bool(true));
            }
            if let Some(exp) = c.expires() {
                if let Ok(dur) = exp.duration_since(SystemTime::UNIX_EPOCH) {
                    entry.insert(
                        HashKey::Str("expires".into()),
                        Value::Float(dur.as_secs_f64()),
                    );
                    entry.insert(HashKey::Str("persistent".into()), Value::Bool(true));
                }
            }
            if let Some(max_age) = c.max_age() {
                entry.insert(
                    HashKey::Str("expires".into()),
                    Value::Float(now + max_age.as_secs_f64()),
                );
                entry.insert(HashKey::Str("max_age".into()), Value::Float(max_age.as_secs_f64()));
                entry.insert(HashKey::Str("persistent".into()), Value::Bool(true));
            }
            let same_site = if c.same_site_strict() {
                Some("strict")
            } else if c.same_site_lax() {
                Some("lax")
            } else {
                None
            };
            if let Some(ss) = same_site {
                entry.insert(HashKey::Str("same_site".into()), Value::Str(ss.into()));
            }
            map.insert(
                HashKey::Str(c.name().to_string()),
                Value::Dict(Rc::new(RefCell::new(entry))),
            );
        }
        Value::Dict(Rc::new(RefCell::new(map)))
    }

    fn merge_cookie_entries(
        target: &Rc<RefCell<HashMap<HashKey, Value>>>,
        cookies: &Value,
        url: &Url,
    ) -> Result<(), RuntimeError> {
        let entries = extract_cookies_from_value(Some(cookies), Some(url))?;
        for c in entries {
            let mut entry = HashMap::new();
            entry.insert(HashKey::Str("value".into()), Value::Str(c.value));
            if let Some(d) = c.domain.clone() {
                entry.insert(HashKey::Str("domain".into()), Value::Str(d));
            }
            if let Some(p) = c.path.clone() {
                entry.insert(HashKey::Str("path".into()), Value::Str(p));
            }
            if c.secure {
                entry.insert(HashKey::Str("secure".into()), Value::Bool(true));
            }
            if c.httponly {
                entry.insert(HashKey::Str("httponly".into()), Value::Bool(true));
            }
            if let Some(exp) = c.expires_at {
                entry.insert(HashKey::Str("expires".into()), Value::Float(exp));
            }
            if let Some(ss) = c.same_site.clone() {
                entry.insert(HashKey::Str("same_site".into()), Value::Str(ss));
            }
            if c.persistent {
                entry.insert(HashKey::Str("persistent".into()), Value::Bool(true));
            }
            if let Some(h) = c.host.clone() {
                entry.insert(HashKey::Str("host".into()), Value::Str(h));
            }
            if c.host_only {
                entry.insert(HashKey::Str("host_only".into()), Value::Bool(true));
            }
            target
                .borrow_mut()
                .insert(HashKey::Str(c.name), Value::Dict(Rc::new(RefCell::new(entry))));
        }
        Ok(())
    }
}
