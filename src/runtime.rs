use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

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
            Value::Function(_) | Value::Class(_) | Value::Instance(_) | Value::BoundMethod(_) => {
                true
            }
            Value::File(_) => true,
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

pub type NativeFunc =
    Rc<dyn Fn(&[Value], &mut Env, &mut HashMap<String, Value>) -> Result<Value, RuntimeError>>;

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
