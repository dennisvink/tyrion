use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::runtime::{RuntimeError};
use crate::{AssignOp, AssignTarget, Expr, ForTarget, Program, Stmt, UnOp};

/// Runtime helpers reused by generated code.
pub mod rt {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::io::{Read, SeekFrom};
    use std::rc::Rc;

    use crate::runtime::{
        BoundMethodValue, Env, FunctionImpl, FunctionValue, HashKey, InstanceValue,
        RuntimeError, Value,
    };

    #[derive(Clone, Debug)]
    pub enum Flow {
        Continue,
        Return(Value),
        Raise(Value),
    }

    pub fn value_to_string(v: &Value) -> String {
        match v {
            Value::None => "None".into(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Str(s) => s.clone(),
            Value::List(items) => {
                let inner: Vec<String> = items.borrow().iter().map(value_to_string).collect();
                format!("[{}]", inner.join(", "))
            }
            Value::Tuple(items) => {
                let inner: Vec<String> = items.iter().map(value_to_string).collect();
                format!("({})", inner.join(", "))
            }
            Value::Dict(map) => {
                let mut kvs: Vec<(String, String)> = map
                    .borrow()
                    .iter()
                    .map(|(k, v)| {
                        let key_str = match k {
                            HashKey::Int(i) => i.to_string(),
                            HashKey::FloatOrdered(b) => f64::from_bits(*b as u64).to_string(),
                            HashKey::Bool(b) => b.to_string(),
                            HashKey::Str(s) => s.clone(),
                        };
                        (key_str, value_to_string(v))
                    })
                    .collect();
                kvs.sort_by(|a, b| a.0.cmp(&b.0));
                format!(
                    "{{{}}}",
                    kvs.into_iter()
                        .map(|(k, v)| format!("{k}: {v}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            Value::Set(set) => {
                let mut vals: Vec<String> = set
                    .borrow()
                    .iter()
                    .map(|k| match k {
                        HashKey::Int(i) => i.to_string(),
                        HashKey::FloatOrdered(b) => f64::from_bits(*b as u64).to_string(),
                        HashKey::Bool(b) => b.to_string(),
                        HashKey::Str(s) => s.clone(),
                    })
                    .collect();
                vals.sort();
                format!("{{{}}}", vals.join(", "))
            }
            Value::Function(f) => format!("<fn {}>", f.name.clone().unwrap_or_else(|| "?".into())),
            Value::Class(c) => format!("<class {}>", c.name),
            Value::Instance(inst) => format!("<instance {}>", inst.class.name),
            Value::BoundMethod(_) => "<bound method>".into(),
            Value::File(f) => format!("<file {}>", f.path),
            Value::Generator(_) => "<generator>".into(),
        }
    }

    pub fn as_int(v: &Value) -> Result<i64, RuntimeError> {
        match v {
            Value::Int(i) => Ok(*i),
            _ => Err(RuntimeError::new("expected int")),
        }
    }

    pub fn as_float(v: &Value) -> Result<f64, RuntimeError> {
        match v {
            Value::Float(f) => Ok(*f),
            Value::Int(i) => Ok(*i as f64),
            _ => Err(RuntimeError::new("expected float")),
        }
    }

    pub fn negate(v: Value) -> Result<Value, RuntimeError> {
        match v {
            Value::Int(i) => Ok(Value::Int(-i)),
            Value::Float(f) => Ok(Value::Float(-f)),
            _ => Err(RuntimeError::new("bad operand for unary -")),
        }
    }

    pub fn binary(lhs: Value, rhs: Value, op: crate::BinOp) -> Result<Value, RuntimeError> {
        match op {
            crate::BinOp::Add => match (lhs, rhs) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
                (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 + b)),
                (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a + b as f64)),
                (Value::Str(a), Value::Str(b)) => Ok(Value::Str(format!("{a}{b}"))),
                (Value::List(a), Value::List(b)) => {
                    let mut merged = a.borrow().clone();
                    merged.extend(b.borrow().iter().cloned());
                    Ok(Value::List(Rc::new(RefCell::new(merged))))
                }
                _ => Err(RuntimeError::new("unsupported +")),
            },
            crate::BinOp::Sub => match (lhs, rhs) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a - b)),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
                (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 - b)),
                (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a - b as f64)),
                _ => Err(RuntimeError::new("unsupported -")),
            },
            crate::BinOp::Mul => match (lhs, rhs) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a * b)),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
                (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 * b)),
                (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a * b as f64)),
                _ => Err(RuntimeError::new("unsupported *")),
            },
            crate::BinOp::Div => match (lhs, rhs) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Float(a as f64 / b as f64)),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a / b)),
                (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 / b)),
                (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a / b as f64)),
                _ => Err(RuntimeError::new("unsupported /")),
            },
        }
    }

    pub fn compare(lhs: Value, rhs: Value, op: crate::CmpOp) -> Result<Value, RuntimeError> {
        let result = match op {
            crate::CmpOp::Eq => value_eq(&lhs, &rhs)?,
            crate::CmpOp::Ne => !value_eq(&lhs, &rhs)?,
            crate::CmpOp::Lt => ordered(&lhs, &rhs, |o| o == std::cmp::Ordering::Less)?,
            crate::CmpOp::Le => ordered(&lhs, &rhs, |o| {
                o == std::cmp::Ordering::Less || o == std::cmp::Ordering::Equal
            })?,
            crate::CmpOp::Gt => ordered(&lhs, &rhs, |o| o == std::cmp::Ordering::Greater)?,
            crate::CmpOp::Ge => ordered(&lhs, &rhs, |o| {
                o == std::cmp::Ordering::Greater || o == std::cmp::Ordering::Equal
            })?,
        };
        Ok(Value::Bool(result))
    }

    fn value_eq(a: &Value, b: &Value) -> Result<bool, RuntimeError> {
        Ok(match (a, b) {
            (Value::Int(x), Value::Int(y)) => x == y,
            (Value::Float(x), Value::Float(y)) => x == y,
            (Value::Bool(x), Value::Bool(y)) => x == y,
            (Value::Str(x), Value::Str(y)) => x == y,
            _ => false,
        })
    }

    fn ordered<F: Fn(std::cmp::Ordering) -> bool>(
        a: &Value,
        b: &Value,
        f: F,
    ) -> Result<bool, RuntimeError> {
        let ord = match (a, b) {
            (Value::Int(x), Value::Int(y)) => x.cmp(y),
            (Value::Float(x), Value::Float(y)) => x
                .partial_cmp(y)
                .ok_or_else(|| RuntimeError::new("NaN compare"))?,
            (Value::Int(x), Value::Float(y)) => (*x as f64)
                .partial_cmp(y)
                .ok_or_else(|| RuntimeError::new("NaN compare"))?,
            (Value::Float(x), Value::Int(y)) => x
                .partial_cmp(&(*y as f64))
                .ok_or_else(|| RuntimeError::new("NaN compare"))?,
            (Value::Str(x), Value::Str(y)) => x.cmp(y),
            _ => return Err(RuntimeError::new("unorderable types")),
        };
        Ok(f(ord))
    }

    pub fn compare_for_sort(a: &Value, b: &Value) -> std::cmp::Ordering {
        match (a, b) {
            (Value::Int(x), Value::Int(y)) => x.cmp(y),
            (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
            (Value::Int(x), Value::Float(y)) => (*x as f64)
                .partial_cmp(y)
                .unwrap_or(std::cmp::Ordering::Equal),
            (Value::Float(x), Value::Int(y)) => x
                .partial_cmp(&(*y as f64))
                .unwrap_or(std::cmp::Ordering::Equal),
            (Value::Str(x), Value::Str(y)) => x.cmp(y),
            _ => std::cmp::Ordering::Equal,
        }
    }

    pub fn iterate_value(v: Value) -> Result<Vec<Value>, RuntimeError> {
        match v {
            Value::List(items) => Ok(items.borrow().clone()),
            Value::Tuple(items) => Ok(items),
            Value::Set(set) => {
                let mut res = Vec::new();
                for k in set.borrow().iter() {
                    res.push(match k {
                        HashKey::Int(i) => Value::Int(*i),
                        HashKey::FloatOrdered(b) => Value::Float(f64::from_bits(*b as u64)),
                        HashKey::Bool(b) => Value::Bool(*b),
                        HashKey::Str(s) => Value::Str(s.clone()),
                    });
                }
                res.sort_by(compare_for_sort);
                Ok(res)
            }
            Value::Dict(map) => {
                let mut vals: Vec<Value> = map
                    .borrow()
                    .iter()
                    .map(|(k, _)| match k {
                        HashKey::Int(i) => Value::Int(*i),
                        HashKey::FloatOrdered(b) => Value::Float(f64::from_bits(*b as u64)),
                        HashKey::Bool(b) => Value::Bool(*b),
                        HashKey::Str(s) => Value::Str(s.clone()),
                    })
                    .collect();
                vals.sort_by(compare_for_sort);
                Ok(vals)
            }
            Value::Str(s) => Ok(s.chars().map(|c| Value::Str(c.to_string())).collect()),
            Value::Generator(_) => Err(RuntimeError::new("generator iteration not supported in AOT")),
            _ => Err(RuntimeError::new("not iterable")),
        }
    }

    pub fn subscript_get(obj: Value, idx: Value) -> Result<Value, RuntimeError> {
        match obj {
            Value::List(items) => match idx {
                Value::Int(i) => {
                    let vec = items.borrow();
                    let ii = fix_index(i, vec.len())?;
                    Ok(vec[ii].clone())
                }
                _ => Err(RuntimeError::new("list index must be int")),
            },
            Value::Tuple(items) => match idx {
                Value::Int(i) => {
                    let ii = fix_index(i, items.len())?;
                    Ok(items[ii].clone())
                }
                _ => Err(RuntimeError::new("tuple index must be int")),
            },
            Value::Dict(map) => {
                let key = HashKey::from_value(&idx).ok_or_else(|| RuntimeError::new("unhashable key"))?;
                map.borrow().get(&key).cloned().ok_or_else(|| RuntimeError::new("key not found"))
            }
            Value::Str(s) => match idx {
                Value::Int(i) => {
                    let chars: Vec<_> = s.chars().collect();
                    let ii = fix_index(i, chars.len())?;
                    Ok(Value::Str(chars[ii].to_string()))
                }
                _ => Err(RuntimeError::new("string index must be int")),
            },
            _ => Err(RuntimeError::new("subscript not supported")),
        }
    }

    pub fn subscript_set(obj: Value, idx: Value, value: Value) -> Result<(), RuntimeError> {
        match obj {
            Value::List(items) => match idx {
                Value::Int(i) => {
                    let mut vec = items.borrow_mut();
                    let ii = fix_index(i, vec.len())?;
                    vec[ii] = value;
                    Ok(())
                }
                _ => Err(RuntimeError::new("list index must be int")),
            },
            Value::Dict(map) => {
                let key = HashKey::from_value(&idx).ok_or_else(|| RuntimeError::new("unhashable key"))?;
                map.borrow_mut().insert(key, value);
                Ok(())
            }
            _ => Err(RuntimeError::new("subscript assign not supported")),
        }
    }

    pub fn slice_value(value: Value, start: Option<Value>, end: Option<Value>) -> Result<Value, RuntimeError> {
        let (s, e) = match &value {
            Value::List(items) => {
                let len = items.borrow().len();
                let s = start.map(|v| clamp_index(as_int(&v).unwrap_or(0), len)).unwrap_or(0);
                let e = end.map(|v| clamp_index(as_int(&v).unwrap_or(len as i64), len)).unwrap_or(len);
                (s, e)
            }
            Value::Str(s) => {
                let len = s.chars().count();
                let s = start.map(|v| clamp_index(as_int(&v).unwrap_or(0), len)).unwrap_or(0);
                let e = end.map(|v| clamp_index(as_int(&v).unwrap_or(len as i64), len)).unwrap_or(len);
                (s, e)
            }
            _ => return Err(RuntimeError::new("slice unsupported")),
        };
        match value {
            Value::List(items) => {
                let vec = items.borrow();
                Ok(Value::List(Rc::new(RefCell::new(vec[s..e].to_vec()))))
            }
            Value::Str(sval) => {
                let chars: Vec<_> = sval.chars().collect();
                let slice: String = chars[s..e].iter().collect();
                Ok(Value::Str(slice))
            }
            _ => Err(RuntimeError::new("slice unsupported")),
        }
    }

    fn clamp_index(idx: i64, len: usize) -> usize {
        let l = len as i64;
        let mut i = idx;
        if i < 0 {
            i = l + i;
        }
        if i < 0 {
            0
        } else if i as usize > len {
            len
        } else {
            i as usize
        }
    }

    fn fix_index(i: i64, len: usize) -> Result<usize, RuntimeError> {
        let l = len as i64;
        let mut idx = i;
        if idx < 0 {
            idx = l + idx;
        }
        if idx < 0 || idx >= l {
            return Err(RuntimeError::new("index out of range"));
        }
        Ok(idx as usize)
    }

    pub fn attr_get(obj: Value, attr: &str) -> Result<Value, RuntimeError> {
        match obj {
            Value::Instance(inst) => {
                if let Some(v) = inst.fields.borrow().get(attr) {
                    return Ok(v.clone());
                }
                if let Some(m) = inst.class.methods.get(attr) {
                    let bm = BoundMethodValue {
                        receiver: Value::Instance(inst.clone()),
                        func: m.clone(),
                    };
                    return Ok(Value::BoundMethod(Box::new(bm)));
                }
                Err(RuntimeError::new("attribute not found"))
            }
            Value::Class(cls) => cls
                .methods
                .get(attr)
                .map(|m| Value::Function(m.clone()))
                .ok_or_else(|| RuntimeError::new("attribute not found")),
            Value::List(list) => match attr {
                "append" => Ok(Value::Function(FunctionValue {
                    name: Some("append".into()),
                    arity: 1,
                    impls: FunctionImpl::Native(Rc::new(move |args, _kwargs, _env, _globals| {
                        let mut vec = list.borrow().clone();
                        vec.push(args[0].clone());
                        Ok(Value::List(Rc::new(RefCell::new(vec))))
                    })),
                })),
                "remove" => Ok(Value::Function(FunctionValue {
                    name: Some("remove".into()),
                    arity: 1,
                    impls: FunctionImpl::Native(Rc::new(move |args, _kwargs, _env, _globals| {
                        let mut vec = list.borrow().clone();
                        if let Some(pos) = vec
                            .iter()
                            .position(|v| values_equal(v, &args[0]).unwrap_or(false))
                        {
                            vec.remove(pos);
                        }
                        Ok(Value::List(Rc::new(RefCell::new(vec))))
                    })),
                })),
                _ => Err(RuntimeError::new("unknown list method")),
            },
            Value::Set(set) => match attr {
                "add" => Ok(Value::Function(FunctionValue {
                    name: Some("add".into()),
                    arity: 1,
                    impls: FunctionImpl::Native(Rc::new(move |args, _kwargs, _env, _globals| {
                        let mut s = set.borrow().clone();
                        let key = HashKey::from_value(&args[0])
                            .ok_or_else(|| RuntimeError::new("unhashable set value"))?;
                        s.insert(key);
                        Ok(Value::Set(Rc::new(RefCell::new(s))))
                    })),
                })),
                _ => Err(RuntimeError::new("unknown set method")),
            },
            Value::Str(s) => match attr {
                "split" => Ok(Value::Function(FunctionValue {
                    name: Some("split".into()),
                    arity: 1,
                    impls: FunctionImpl::Native(Rc::new(move |args, _kwargs, _env, _globals| {
                        let delim = match args.get(0) {
                            Some(Value::Str(d)) => d.clone(),
                            None => " ".into(),
                            _ => return Err(RuntimeError::new("split delim must be str")),
                        };
                        let parts: Vec<Value> =
                            s.split(&delim).map(|p| Value::Str(p.to_string())).collect();
                        Ok(Value::List(Rc::new(RefCell::new(parts))))
                    })),
                })),
                "strip" => Ok(Value::Function(FunctionValue {
                    name: Some("strip".into()),
                    arity: 0,
                    impls: FunctionImpl::Native(Rc::new(move |_args, _kwargs, _env, _globals| {
                        Ok(Value::Str(s.trim().to_string()))
                    })),
                })),
                "startswith" => Ok(Value::Function(FunctionValue {
                    name: Some("startswith".into()),
                    arity: 1,
                    impls: FunctionImpl::Native(Rc::new(move |args, _kwargs, _env, _globals| {
                        let pref = match args.get(0) {
                            Some(Value::Str(d)) => d,
                            _ => return Err(RuntimeError::new("startswith needs str")),
                        };
                        Ok(Value::Bool(s.starts_with(pref.as_str())))
                    })),
                })),
                "endswith" => Ok(Value::Function(FunctionValue {
                    name: Some("endswith".into()),
                    arity: 1,
                    impls: FunctionImpl::Native(Rc::new(move |args, _kwargs, _env, _globals| {
                        let suf = match args.get(0) {
                            Some(Value::Str(d)) => d,
                            _ => return Err(RuntimeError::new("endswith needs str")),
                        };
                        Ok(Value::Bool(s.ends_with(suf.as_str())))
                    })),
                })),
                _ => Err(RuntimeError::new("unknown str method")),
            },
            Value::Dict(map) => match attr {
                "keys" => Ok(Value::Function(FunctionValue {
                    name: Some("keys".into()),
                    arity: 0,
                    impls: FunctionImpl::Native(Rc::new(move |_args, _kwargs, _env, _globals| {
                        let mut vals: Vec<Value> = map
                            .borrow()
                            .keys()
                            .map(|k| match k {
                                HashKey::Int(i) => Value::Int(*i),
                                HashKey::FloatOrdered(b) => Value::Float(f64::from_bits(*b as u64)),
                                HashKey::Bool(b) => Value::Bool(*b),
                                HashKey::Str(s) => Value::Str(s.clone()),
                            })
                            .collect();
                        vals.sort_by(compare_for_sort);
                        Ok(Value::List(Rc::new(RefCell::new(vals))))
                    })),
                })),
                "items" => Ok(Value::Function(FunctionValue {
                    name: Some("items".into()),
                    arity: 0,
                    impls: FunctionImpl::Native(Rc::new(move |_args, _kwargs, _env, _globals| {
                        let mut vals = Vec::new();
                        for (k, v) in map.borrow().iter() {
                            let key_val = match k {
                                HashKey::Int(i) => Value::Int(*i),
                                HashKey::FloatOrdered(b) => Value::Float(f64::from_bits(*b as u64)),
                                HashKey::Bool(b) => Value::Bool(*b),
                                HashKey::Str(s) => Value::Str(s.clone()),
                            };
                            vals.push(Value::Tuple(vec![key_val, v.clone()]));
                        }
                        vals.sort_by(|a, b| {
                            if let (Value::Tuple(ka), Value::Tuple(kb)) = (a, b) {
                                compare_for_sort(&ka[0], &kb[0])
                            } else {
                                std::cmp::Ordering::Equal
                            }
                        });
                        Ok(Value::List(Rc::new(RefCell::new(vals))))
                    })),
                })),
                _ => Err(RuntimeError::new("unknown dict method")),
            },
            Value::File(file) => match attr {
                "read" => Ok(Value::Function(FunctionValue {
                    name: Some("read".into()),
                    arity: 1,
                    impls: FunctionImpl::Native(Rc::new(move |args, _kwargs, _env, _globals| {
                        let mut f = file.file.borrow_mut();
                        let mut buf = String::new();
                        if let Some(Value::Int(n)) = args.get(0) {
                            let mut tmp = vec![0u8; *n as usize];
                            let read_n = f
                                .read(&mut tmp)
                                .map_err(|e| RuntimeError::new(e.to_string()))?;
                            buf.push_str(&String::from_utf8_lossy(&tmp[..read_n]));
                        } else {
                            f.read_to_string(&mut buf)
                                .map_err(|e| RuntimeError::new(e.to_string()))?;
                        }
                        Ok(Value::Str(buf))
                    })),
                })),
                "write" => Ok(Value::Function(FunctionValue {
                    name: Some("write".into()),
                    arity: 1,
                    impls: FunctionImpl::Native(Rc::new(move |args, _kwargs, _env, _globals| {
                        use std::io::Write;
                        let mut f = file.file.borrow_mut();
                        let s = match &args[0] {
                            Value::Str(s) => s.clone(),
                            other => value_to_string(other),
                        };
                        f.write_all(s.as_bytes())
                            .map_err(|e| RuntimeError::new(e.to_string()))?;
                        Ok(Value::None)
                    })),
                })),
                "seek" => Ok(Value::Function(FunctionValue {
                    name: Some("seek".into()),
                    arity: 2,
                    impls: FunctionImpl::Native(Rc::new(move |args, _kwargs, _env, _globals| {
                        use std::io::Seek;
                        let offset = match args.get(0) {
                            Some(Value::Int(i)) => *i,
                            _ => return Err(RuntimeError::new("seek offset must be int")),
                        };
                        let whence = match args.get(1) {
                            Some(Value::Int(i)) => *i,
                            _ => return Err(RuntimeError::new("seek whence must be int")),
                        };
                        let seek_from = match whence {
                            crate::runtime::SEEK_SET => SeekFrom::Start(offset as u64),
                            crate::runtime::SEEK_CUR => SeekFrom::Current(offset),
                            crate::runtime::SEEK_END => SeekFrom::End(offset),
                            _ => return Err(RuntimeError::new("invalid whence")),
                        };
                        file.file
                            .borrow_mut()
                            .seek(seek_from)
                            .map_err(|e| RuntimeError::new(e.to_string()))?;
                        Ok(Value::None)
                    })),
                })),
                _ => Err(RuntimeError::new("unknown file method")),
            },
            _ => Err(RuntimeError::new("attribute access not supported")),
        }
    }

    pub fn attr_set(obj: Value, attr: &str, value: Value) -> Result<(), RuntimeError> {
        match obj {
            Value::Instance(inst) => {
                inst.fields.borrow_mut().insert(attr.to_string(), value);
                Ok(())
            }
            _ => Err(RuntimeError::new("attribute set not supported")),
        }
    }

    pub fn call_value(
        callee: Value,
        mut args: Vec<Value>,
        kwargs: Vec<(String, Value)>,
        env: &mut Env,
        globals: &mut HashMap<String, Value>,
    ) -> Result<Value, RuntimeError> {
        match callee {
            Value::Function(f) => match f.impls {
                FunctionImpl::Native(func) => func(&args, &kwargs, env, globals),
                FunctionImpl::User(_) => Err(RuntimeError::new("user function in AOT should be native")),
            },
            Value::BoundMethod(b) => {
                args.insert(0, b.receiver.clone());
                call_value(Value::Function(b.func), args, kwargs, env, globals)
            }
            Value::Class(cls) => {
                let inst = Value::Instance(InstanceValue {
                    class: Rc::new(cls.clone()),
                    fields: Rc::new(std::cell::RefCell::new(HashMap::new())),
                });
                if let Some(init) = cls.methods.get("__init__") {
                    let bm = BoundMethodValue {
                        receiver: inst.clone(),
                        func: init.clone(),
                    };
                    call_value(Value::BoundMethod(Box::new(bm)), args, kwargs, env, globals)?;
                }
                Ok(inst)
            }
            _ => Err(RuntimeError::new("cannot call value")),
        }
    }

    pub fn values_equal(a: &Value, b: &Value) -> Result<bool, RuntimeError> {
        match (a, b) {
            (Value::Int(x), Value::Int(y)) => Ok(x == y),
            (Value::Float(x), Value::Float(y)) => Ok(*x == *y),
            (Value::Bool(x), Value::Bool(y)) => Ok(x == y),
            (Value::Str(x), Value::Str(y)) => Ok(x == y),
            _ => Ok(false),
        }
    }
}

struct Codegen {
    buf: String,
    indent: usize,
    fn_counter: usize,
    fn_map: HashMap<String, String>,
}

impl Codegen {
    fn new() -> Self {
        Codegen {
            buf: String::new(),
            indent: 0,
            fn_counter: 0,
            fn_map: HashMap::new(),
        }
    }

    fn indent(&mut self) {
        self.indent += 1;
    }

    fn dedent(&mut self) {
        self.indent -= 1;
    }

    fn line<S: AsRef<str>>(&mut self, s: S) {
        for _ in 0..self.indent {
            self.buf.push_str("    ");
        }
        self.buf.push_str(s.as_ref());
        self.buf.push('\n');
    }

    fn fresh(&mut self, prefix: &str) -> String {
        let name = format!("{prefix}{}", self.fn_counter);
        self.fn_counter += 1;
        name
    }

    fn register_fn(&mut self, logical: String, rust: String) {
        self.fn_map.insert(logical, rust);
    }
}

pub fn compile_aot(program: &Program, output_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let crate_root = resolve_crate_root()?;
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let runner_dir = crate_root
        .join("target")
        .join(format!("tyrion_aot_{stamp}"));
    fs::create_dir_all(runner_dir.join("src"))?;

    let runner_cargo = format!(
        "[package]\nname = \"tyrion_aot_runner\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dependencies]\ntyrion = {{ path = \"{}\" }}\n",
        crate_root.display()
    );
    fs::write(runner_dir.join("Cargo.toml"), runner_cargo)?;

    let mut cg = Codegen::new();
    emit_program(&mut cg, program)?;
    fs::write(runner_dir.join("src").join("main.rs"), cg.buf)?;

    let status = Command::new("cargo")
        .arg("build")
        .arg("--release")
        .current_dir(&runner_dir)
        .status()?;
    if !status.success() {
        return Err("cargo build failed".into());
    }

    let built = runner_dir
        .join("target")
        .join("release")
        .join("tyrion_aot_runner");
    if !built.exists() {
        return Err("built binary not found".into());
    }
    fs::copy(&built, output_path)?;
    Ok(())
}

fn resolve_crate_root() -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Ok(root) = env::var("TYRION_CRATE_ROOT") {
        let path = PathBuf::from(root);
        if path.join("Cargo.toml").exists() {
            return Ok(path.canonicalize()?);
        }
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    if manifest_dir.join("Cargo.toml").exists() {
        return Ok(manifest_dir.canonicalize()?);
    }

    let cwd = env::current_dir()?;
    if cwd.join("Cargo.toml").exists() {
        return Ok(cwd.canonicalize()?);
    }

    Err("could not locate tyrion crate root; set TYRION_CRATE_ROOT to the repo path".into())
}

fn emit_program(cg: &mut Codegen, program: &Program) -> Result<(), RuntimeError> {
    cg.line("#![allow(unused_imports, unused_mut, unreachable_code, unused_assignments)]");
    cg.line("use std::collections::HashMap;");
    cg.line("use std::rc::Rc;");
    cg.line("use tyrion::runtime::{Value, HashKey, RuntimeError, FunctionValue, FunctionImpl, ClassValue, InstanceValue, BoundMethodValue, Env};");
    cg.line("use tyrion::runtime::http; // ensure HTTP runtime (requests) is linked for AOT");
    cg.line("use tyrion::aot::rt;");
    cg.line("use tyrion::aot::rt::*;");
    cg.line("use tyrion::interpreter;");
    cg.line("");

    // gather functions and methods
    let mut fn_defs = Vec::new();
    collect_functions(program, &mut fn_defs, cg);
    for def in &fn_defs {
        emit_fn(cg, def)?;
    }

    cg.line("fn main() -> Result<(), RuntimeError> {");
    cg.indent();
    cg.line("let mut globals: HashMap<String, Value> = interpreter::builtins();");
    cg.line("// Touch requests module so it is retained in AOT binaries");
    cg.line("let _ = http::build_requests_module as fn() -> Value;");
    cg.line("let mut env = Env::new();");
    for stmt in &program.stmts {
        emit_stmt(cg, stmt, "globals", "env")?;
    }
    cg.line("Ok(())");
    cg.line("}");
    Ok(())
}

#[derive(Clone)]
struct FnDef {
    rust_name: String,
    func_name: String,
    params: Vec<String>,
    body: Vec<Stmt>,
}

fn collect_functions(program: &Program, out: &mut Vec<FnDef>, cg: &mut Codegen) {
    for stmt in &program.stmts {
        match stmt {
            Stmt::Function { name, params, body, .. } => {
                let rust_name = cg.fresh("fn_user_");
                cg.register_fn(name.clone(), rust_name.clone());
                out.push(FnDef {
                    rust_name,
                    func_name: name.clone(),
                    params: params.iter().map(|p| p.name.clone()).collect(),
                    body: body.clone(),
                });
            }
            Stmt::Class { name, methods } => {
                for m in methods {
                    if let Stmt::Function { name: mname, params, body, .. } = m {
                        let func_name = format!("{name}_{}", mname);
                        let rust_name = cg.fresh("fn_method_");
                        cg.register_fn(func_name.clone(), rust_name.clone());
                        out.push(FnDef {
                            rust_name,
                            func_name,
                            params: params.iter().map(|p| p.name.clone()).collect(),
                            body: body.clone(),
                        });
                    }
                }
            }
            _ => {}
        }
    }
}

fn emit_fn(cg: &mut Codegen, def: &FnDef) -> Result<(), RuntimeError> {
    cg.line(format!(
        "fn {}(args: &[Value], env: &mut Env, globals: &mut HashMap<String, Value>) -> Result<Value, RuntimeError> {{",
        def.rust_name
    ));
    cg.indent();
    cg.line("let mut local = env.child();");
    for (idx, p) in def.params.iter().enumerate() {
        cg.line(format!("let arg_{idx} = args.get({idx}).cloned().ok_or_else(|| RuntimeError::new(\"missing arg\"))?;"));
        cg.line(format!("local.set(\"{}\", arg_{});", p, idx));
    }
    for stmt in &def.body {
        emit_fn_stmt(cg, stmt, "globals", "local")?;
    }
    cg.line("Ok(Value::None)");
    cg.dedent();
    cg.line("}");
    cg.line("");
    Ok(())
}

fn emit_fn_stmt(cg: &mut Codegen, stmt: &Stmt, globals: &str, env: &str) -> Result<(), RuntimeError> {
    match stmt {
        Stmt::Return { expr } => {
            if let Some(e) = expr {
                let ex = emit_expr(cg, e, globals, env)?;
                cg.line(format!("return {};", ex));
            } else {
                cg.line("return Ok(Value::None);");
            }
        }
        Stmt::Yield { .. } => {
            return Err(RuntimeError::new("yield not supported in AOT"));
        }
        _ => emit_stmt(cg, stmt, globals, env)?,
    }
    Ok(())
}

fn emit_stmt(cg: &mut Codegen, stmt: &Stmt, globals: &str, env: &str) -> Result<(), RuntimeError> {
    match stmt {
        Stmt::Assign { target, expr, op } => {
            let val_expr = emit_expr(cg, expr, globals, env)?;
            match op {
                AssignOp::Assign => emit_store_target(cg, target, &val_expr, globals, env)?,
                _ => {
                    let current = emit_load_target(cg, target, globals, env)?;
                    let res_tmp = cg.fresh("tmp");
                    let op_expr = match op {
                        AssignOp::Add => format!("rt::binary({current}?, {val_expr}?, tyrion::BinOp::Add)"),
                        AssignOp::Sub => format!("rt::binary({current}?, {val_expr}?, tyrion::BinOp::Sub)"),
                        AssignOp::Mul => format!("rt::binary({current}?, {val_expr}?, tyrion::BinOp::Mul)"),
                        AssignOp::Div => format!("rt::binary({current}?, {val_expr}?, tyrion::BinOp::Div)"),
                        AssignOp::Assign => unreachable!(),
                    };
                    cg.line(format!("let {res_tmp} = {op_expr}?;"));
                    emit_store_target(cg, target, &format!("Ok({res_tmp}.clone())"), globals, env)?;
                }
            }
        }
        Stmt::Print { args } => {
            let mut parts = Vec::new();
            for a in args {
                parts.push(emit_expr(cg, a, globals, env)?);
            }
            let joined = parts
                .into_iter()
                .map(|p| format!("rt::value_to_string(&({p}?))"))
                .collect::<Vec<_>>()
                .join(", ");
            cg.line(format!("println!(\"{{}}\", vec![{joined}].join(\" \"));"));
        }
        Stmt::ExprStmt { expr } => {
            let ex = emit_expr(cg, expr, globals, env)?;
            cg.line(format!("let _ = {ex}?;"));
        }
        Stmt::If { branches, else_branch } => {
            let mut first = true;
            for (cond, body) in branches {
                let cex = emit_expr(cg, cond, globals, env)?;
                if first {
                    cg.line(format!("if ({cex}?).truthy() {{"));
                    first = false;
                } else {
                    cg.line(format!("}} else if ({cex}?).truthy() {{"));
                }
                cg.indent();
                for s in body {
                    emit_stmt(cg, s, globals, env)?;
                }
                cg.dedent();
            }
            if let Some(body) = else_branch {
                cg.line("} else {");
                cg.indent();
                for s in body {
                    emit_stmt(cg, s, globals, env)?;
                }
                cg.dedent();
            }
            cg.line("}");
        }
        Stmt::While { cond, body } => {
            let cex = emit_expr(cg, cond, globals, env)?;
            cg.line("loop {");
            cg.indent();
            cg.line(format!("if !({}?).truthy() {{ break; }}", cex));
            for s in body {
                emit_stmt(cg, s, globals, env)?;
            }
            cg.dedent();
            cg.line("}");
        }
        Stmt::For { target, iter, body } => {
            let iter_expr = emit_expr(cg, iter, globals, env)?;
            let items_tmp = cg.fresh("items");
            cg.line(format!("let {items_tmp} = rt::iterate_value({iter_expr}?)?;"));
            cg.line(format!("for item in {items_tmp} {{"));
            cg.indent();
            emit_bind_for_target(cg, target, "item", env)?;
            for s in body {
                emit_stmt(cg, s, globals, env)?;
            }
            cg.dedent();
            cg.line("}");
        }
        Stmt::With { target, expr, body } => {
            let ex = emit_expr(cg, expr, globals, env)?;
            let tmp = cg.fresh("withv");
            cg.line(format!("let {tmp} = {ex}?;"));
            cg.line(format!("{env}.set(\"{target}\", {tmp}.clone());"));
            for s in body {
                emit_stmt(cg, s, globals, env)?;
            }
        }
        Stmt::Return { expr } => {
            if let Some(e) = expr {
                let ex = emit_expr(cg, e, globals, env)?;
                cg.line(format!("return {ex};"));
            } else {
                cg.line("return Ok(Value::None);");
            }
        }
        Stmt::Yield { .. } => {
            return Err(RuntimeError::new("yield not supported in AOT"));
        }
        Stmt::Function { name, params, .. } => {
            let names: Vec<String> = params.iter().map(|p| p.name.clone()).collect();
            let fn_name = find_fn_name(name, &names, cg)?;
            cg.line(format!(
                "let func = Value::Function(FunctionValue {{ name: Some(\"{}\".into()), arity: {}, impls: FunctionImpl::Native(Rc::new(|args, kwargs, env, globals| {}(args, env, globals))) }});",
                name,
                params.len(),
                fn_name
            ));
            cg.line(format!("{env}.set(\"{name}\", func.clone());"));
            cg.line(format!("{globals}.insert(\"{name}\".into(), func);"));
        }
        Stmt::Class { name, methods } => {
            cg.line("let mut method_map = HashMap::new();");
            for m in methods {
                if let Stmt::Function { name: mname, params, .. } = m {
                    let names: Vec<String> = params.iter().map(|p| p.name.clone()).collect();
                    let fn_name = find_fn_name(&format!("{name}_{mname}"), &names, cg)?;
                    cg.line(format!("method_map.insert(\"{mname}\".into(), FunctionValue {{ name: Some(\"{mname}\".into()), arity: {}, impls: FunctionImpl::Native(Rc::new(|args, kwargs, env, globals| {}(args, env, globals))) }});", params.len(), fn_name));
                }
            }
            cg.line(format!("let cls = Value::Class(ClassValue {{ name: \"{name}\".into(), methods: method_map }});"));
            cg.line(format!("{env}.set(\"{name}\", cls.clone());"));
            cg.line(format!("{globals}.insert(\"{name}\".into(), cls);"));
        }
        Stmt::Try { body, handlers, finally_body } => {
            cg.line("{");
            cg.indent();
            cg.line("let mut tmp_env = env.child();");
            cg.line("let body_res: Result<(), RuntimeError> = (|| {");
            cg.indent();
            for s in body {
                emit_stmt(cg, s, globals, "tmp_env")?;
            }
            cg.line("Ok(())");
            cg.dedent();
            cg.line("})();");
            cg.line("let mut handled = false;");
            cg.line("let mut err_val: Option<Value> = None;");
            cg.line("if let Err(e) = body_res { err_val = Some(Value::Str(e.message.clone())); }");
            cg.line("if let Some(err) = err_val.clone() {");
            cg.indent();
            for h in handlers {
                if let Some(name) = &h.name {
                    cg.line(format!("if !handled && rt::value_to_string(&err) == \"{}\" {{", name));
                } else {
                    cg.line("if !handled {");
                }
                cg.indent();
                cg.line("handled = true;");
                if let Some(bind) = &h.bind {
                    cg.line(format!("tmp_env.set(\"{bind}\", err.clone());"));
                }
                for s in &h.body {
                    emit_stmt(cg, s, globals, "tmp_env")?;
                }
                cg.dedent();
                cg.line("}");
            }
            cg.dedent();
            cg.line("}");
            if let Some(fin) = finally_body {
                for s in fin {
                    emit_stmt(cg, s, globals, env)?;
                }
            }
            cg.dedent();
            cg.line("}");
        }
        Stmt::Raise { expr } => {
            let ex = emit_expr(cg, expr, globals, env)?;
            cg.line(format!("return Err(RuntimeError::new(rt::value_to_string(&({ex}?))));"));
        }
    }
    Ok(())
}

fn emit_load_target(cg: &mut Codegen, target: &AssignTarget, globals: &str, env: &str) -> Result<String, RuntimeError> {
    match target {
        AssignTarget::Name(n) => Ok(format!("{{ {}.get(\"{}\").or_else(|| {}.get(\"{}\").cloned()).ok_or_else(|| RuntimeError::new(\"Name not found: {}\")) }}", env, n, globals, n, n)),
        AssignTarget::Subscript { object, index } => {
            let o = emit_expr(cg, object, globals, env)?;
            let i = emit_expr(cg, index, globals, env)?;
            Ok(format!("rt::subscript_get({o}?, {i}?)"))
        }
        AssignTarget::Attr { object, attr } => {
            let o = emit_expr(cg, object, globals, env)?;
            Ok(format!("rt::attr_get({o}?, \"{attr}\")"))
        }
        AssignTarget::Tuple { .. } | AssignTarget::Starred(_) => {
            Err(RuntimeError::new("load not supported on tuple target directly"))
        }
    }
}

fn emit_store_target(cg: &mut Codegen, target: &AssignTarget, value_expr: &str, globals: &str, env: &str) -> Result<(), RuntimeError> {
    match target {
        AssignTarget::Name(n) => {
            let tmp = cg.fresh("val");
            cg.line(format!("let {tmp} = ({value_expr})?;"));
            cg.line(format!("{env}.set(\"{n}\", {tmp});"));
        }
        AssignTarget::Subscript { object, index } => {
            let o = emit_expr(cg, object, globals, env)?;
            let i = emit_expr(cg, index, globals, env)?;
            cg.line(format!("rt::subscript_set({o}?, {i}?, ({value_expr})?)?;"));
        }
        AssignTarget::Attr { object, attr } => {
            let o = emit_expr(cg, object, globals, env)?;
            cg.line(format!("rt::attr_set({o}?, \"{attr}\", ({value_expr})?)?;"));
        }
        AssignTarget::Tuple { items } => {
            let tmp = cg.fresh("tup");
            let vals = cg.fresh("iter_vals");
            cg.line(format!("let {tmp} = ({value_expr})?;"));
            cg.line(format!("let {vals} = rt::iterate_value({tmp}.clone())?;"));
            if let Some(star_idx) = items.iter().position(|t| matches!(t, AssignTarget::Starred(_))) {
                let before = star_idx;
                let after = items.len() - star_idx - 1;
                cg.line(format!(
                    "if {vals}.len() < {} {{ return Err(RuntimeError::new(\"unpack arity\")); }}",
                    before + after
                ));
                // leading elements
                for (idx, t) in items.iter().enumerate().take(before) {
                    let val_ref = format!("Ok({vals}[{idx}].clone())");
                    emit_store_target(cg, t, &val_ref, globals, env)?;
                }
                // starred capture
                if let AssignTarget::Starred(name) = &items[star_idx] {
                    let star_tmp = cg.fresh("star");
                    let end_expr = if after == 0 {
                        format!("{vals}.len()")
                    } else {
                        format!("{vals}.len() - {after}")
                    };
                    cg.line(format!(
                        "let {star_tmp}: Vec<Value> = {vals}[{before}..{end_expr}].to_vec();"
                    ));
                    let star_val =
                        format!("Ok(Value::List(Rc::new(std::cell::RefCell::new({star_tmp})) ))");
                    emit_store_target(cg, &AssignTarget::Starred(name.clone()), &star_val, globals, env)?;
                }
                // trailing elements
                for (offset, t) in items.iter().skip(star_idx + 1).enumerate() {
                    let idx_expr = format!("{vals}.len() - {after} + {offset}");
                    let val_ref = format!("Ok({vals}[{idx_expr}].clone())");
                    emit_store_target(cg, t, &val_ref, globals, env)?;
                }
            } else {
                cg.line(format!(
                    "if {vals}.len() != {} {{ return Err(RuntimeError::new(\"unpack arity\")); }}",
                    items.len()
                ));
                for (idx, t) in items.iter().enumerate() {
                    let val_ref = format!("Ok({vals}[{idx}].clone())");
                    emit_store_target(cg, t, &val_ref, globals, env)?;
                }
            }
        }
        AssignTarget::Starred(name) => {
            cg.line(format!("{env}.set(\"{name}\", ({value_expr})?);"));
        }
    }
    Ok(())
}

fn emit_bind_for_target(cg: &mut Codegen, target: &ForTarget, value_expr: &str, env: &str) -> Result<(), RuntimeError> {
    match target {
        ForTarget::Name(n) => {
            cg.line(format!("{env}.set(\"{n}\", {value_expr}.clone());"));
        }
        ForTarget::Tuple(items) => {
            cg.line(format!("if let Value::Tuple(vals) = {value_expr}.clone() {{"));
            cg.indent();
            cg.line(format!("if vals.len() != {} {{ return Err(RuntimeError::new(\"unpack arity\")); }}", items.len()));
            for (idx, n) in items.iter().enumerate() {
                cg.line(format!("{env}.set(\"{n}\", vals[{idx}].clone());"));
            }
            cg.dedent();
            cg.line("} else { return Err(RuntimeError::new(\"expected tuple in for\")); }");
        }
    }
    Ok(())
}

fn emit_expr(cg: &mut Codegen, expr: &Expr, globals: &str, env: &str) -> Result<String, RuntimeError> {
    let out = match expr {
        Expr::Int(i) => format!("Ok(Value::Int({}))", i),
        Expr::Float(f) => format!("Ok(Value::Float({}f64))", f),
        Expr::Str(s) => format!("Ok(Value::Str({:?}.into()))", s),
        Expr::Bool(b) => format!("Ok(Value::Bool({}))", b),
        Expr::Var(name) => format!(
            "{{ {}.get(\"{}\").or_else(|| {}.get(\"{}\").cloned()).ok_or_else(|| RuntimeError::new(\"Name not found: {}\")) }}",
            env, name, globals, name, name
        ),
        Expr::Lambda { params, body } => {
            let fname = cg.fresh("lambda_fn_");
            let def = FnDef {
                rust_name: fname.clone(),
                func_name: fname.clone(),
                params: params.clone(),
                body: vec![Stmt::Return { expr: Some(*body.clone()) }],
            };
            cg.register_fn(def.func_name.clone(), def.rust_name.clone());
            emit_fn(cg, &def)?;
            format!(
                "Ok(Value::Function(FunctionValue {{ name: None, arity: {}, impls: FunctionImpl::Native(Rc::new(|args, kwargs, env, globals| {}(args, env, globals))) }}))",
                params.len(),
                fname
            )
        }
        Expr::Unary(op, e) => {
            let ex = emit_expr(cg, e, globals, env)?;
            match op {
                UnOp::Neg => format!("rt::negate({ex}?)"),
                UnOp::Not => format!("Ok(Value::Bool(!({ex}?).truthy()))"),
            }
        }
        Expr::Binary(l, op, r) => {
            let le = emit_expr(cg, l, globals, env)?;
            let re = emit_expr(cg, r, globals, env)?;
            format!("rt::binary({le}?, {re}?, tyrion::BinOp::{:?})", op)
        }
        Expr::Compare(l, op, r) => {
            let le = emit_expr(cg, l, globals, env)?;
            let re = emit_expr(cg, r, globals, env)?;
            format!("rt::compare({le}?, {re}?, tyrion::CmpOp::{:?})", op)
        }
        Expr::Call {
            callee,
            args,
            kwargs,
        } => {
            let cal = emit_expr(cg, callee, globals, env)?;
            let mut argv = Vec::new();
            for a in args {
                argv.push(emit_expr(cg, a, globals, env)?);
            }
            let mut kw_list = Vec::new();
            for (k, v) in kwargs {
                let vexpr = emit_expr(cg, v, globals, env)?;
                kw_list.push(format!("(\"{}\".to_string(), {vexpr}?)", k));
            }
            let args_list = argv
                .into_iter()
                .map(|a| format!("{a}?"))
                .collect::<Vec<_>>()
                .join(", ");
            let kw_joined = kw_list.join(", ");
            format!(
                "rt::call_value({cal}?, vec![{args_list}], vec![{kw_joined}], &mut {env}, &mut {globals})"
            )
        }
        Expr::List(items) => {
            let mut vals = Vec::new();
            for i in items {
                vals.push(emit_expr(cg, i, globals, env)?);
            }
            let joined = vals.into_iter().map(|v| format!("{v}?")).collect::<Vec<_>>().join(", ");
            format!("Ok(Value::List(Rc::new(std::cell::RefCell::new(vec![{joined}]))))")
        }
        Expr::Tuple(items) => {
            let mut vals = Vec::new();
            for i in items {
                vals.push(emit_expr(cg, i, globals, env)?);
            }
            let joined = vals.into_iter().map(|v| format!("{v}?")).collect::<Vec<_>>().join(", ");
            format!("Ok(Value::Tuple(vec![{joined}]))")
        }
        Expr::Dict(entries) => {
            let mut parts = Vec::new();
            for (k, v) in entries {
                let kk = emit_expr(cg, k, globals, env)?;
                let vv = emit_expr(cg, v, globals, env)?;
                parts.push(format!("(HashKey::from_value(&({kk}?)).ok_or_else(|| RuntimeError::new(\"unhashable dict key\"))?, {vv}?)"));
            }
            let inner = parts.join(", ");
            format!("Ok(Value::Dict(Rc::new(std::cell::RefCell::new(std::collections::HashMap::from([{inner}])))))")
        }
        Expr::Set(items) => {
            let mut parts = Vec::new();
            for i in items {
                let vv = emit_expr(cg, i, globals, env)?;
                parts.push(format!("HashKey::from_value(&({vv}?)).ok_or_else(|| RuntimeError::new(\"unhashable set value\"))?"));
            }
            let inner = parts.join(", ");
            format!("Ok(Value::Set(Rc::new(std::cell::RefCell::new(std::collections::HashSet::from([{inner}])))))")
        }
        Expr::Index(obj, idx) => {
            let o = emit_expr(cg, obj, globals, env)?;
            let i = emit_expr(cg, idx, globals, env)?;
            format!("rt::subscript_get({o}?, {i}?)")
        }
        Expr::Slice { value, start, end } => {
            let v = emit_expr(cg, value, globals, env)?;
            let s = if let Some(se) = start { emit_expr(cg, se, globals, env)? } else { "Ok(Value::None)".into() };
            let e = if let Some(en) = end { emit_expr(cg, en, globals, env)? } else { "Ok(Value::None)".into() };
            format!("rt::slice_value({v}?, {}, {})", match start { Some(_) => format!("Some({s}?)"), None => "None".into() }, match end { Some(_) => format!("Some({e}?)"), None => "None".into() })
        }
        Expr::MethodCall { object, method, args: arg, kwargs } => {
            let o = emit_expr(cg, object, globals, env)?;
            let callee = format!("rt::attr_get({o}?, \"{method}\")");
            let mut argv = Vec::new();
            for a in arg {
                argv.push(emit_expr(cg, a, globals, env)?);
            }
            let mut kwarg_vals = Vec::new();
            for (k, v) in kwargs {
                let ev = emit_expr(cg, v, globals, env)?;
                kwarg_vals.push((k.clone(), ev));
            }
            let args_list = argv.into_iter().map(|a| format!("{a}?")).collect::<Vec<_>>().join(", ");
            let kwargs_list = kwarg_vals.into_iter().map(|(k, v)| format!("(\"{}\".into(), {v}?)", k)).collect::<Vec<_>>().join(", ");
            format!("rt::call_value({callee}?, vec![{args_list}], vec![{kwargs_list}], &mut {env}, &mut {globals})")
        }
        Expr::Attr { object, attr } => {
            let o = emit_expr(cg, object, globals, env)?;
            format!("rt::attr_get({o}?, \"{attr}\")")
        }
        Expr::ListComp { target, iter, expr, cond } => {
            let iter_expr = emit_expr(cg, iter, globals, env)?;
            let tmp_items = cg.fresh("iter");
            let tmp_out = cg.fresh("out");
            let bind_code = bind_for_target_inline(target, "item", env)?;
            let cond_code = if let Some(c) = cond {
                let ce = emit_expr(cg, c, globals, env)?;
                format!("if !({ce}?).truthy() {{ continue; }}")
            } else {
                String::new()
            };
            let inner_expr = emit_expr(cg, expr, globals, env)?;
            let block = format!(
                "{{ let mut {tmp_out} = Vec::new(); let {tmp_items} = rt::iterate_value({iter_expr}?)?; for item in {tmp_items} {{ {bind_code} {cond_code} let v = {inner_expr}?; {tmp_out}.push(v); }} Value::List(Rc::new(std::cell::RefCell::new({tmp_out}))) }}",
            );
            format!("Ok({block})")
        }
        Expr::DictComp { target, iter, key, value, cond } => {
            let iter_expr = emit_expr(cg, iter, globals, env)?;
            let tmp_items = cg.fresh("iter");
            let tmp_out = cg.fresh("out");
            let bind_code = bind_for_target_inline(target, "item", env)?;
            let cond_code = if let Some(c) = cond {
                let ce = emit_expr(cg, c, globals, env)?;
                format!("if !({ce}?).truthy() {{ continue; }}")
            } else {
                String::new()
            };
            let key_expr = emit_expr(cg, key, globals, env)?;
            let val_expr = emit_expr(cg, value, globals, env)?;
            let block = format!(
                "{{ let mut {tmp_out} = std::collections::HashMap::new(); let {tmp_items} = rt::iterate_value({iter_expr}?)?; for item in {tmp_items} {{ {bind_code} {cond_code} let k = HashKey::from_value(&({key_expr}?)).ok_or_else(|| RuntimeError::new(\"unhashable dict key\"))?; let v = {val_expr}?; {tmp_out}.insert(k, v); }} Value::Dict(Rc::new(std::cell::RefCell::new({tmp_out}))) }}",
            );
            format!("Ok({block})")
        }
    };
    Ok(out)
}

fn bind_for_target_inline(target: &ForTarget, value_expr: &str, env: &str) -> Result<String, RuntimeError> {
    let code = match target {
        ForTarget::Name(n) => format!("{env}.set(\"{n}\", {value_expr}.clone());"),
        ForTarget::Tuple(items) => {
            let mut lines = String::new();
            lines.push_str(&format!("if let Value::Tuple(vals) = {value_expr}.clone() {{ if vals.len() != {} {{ return Err(RuntimeError::new(\"unpack arity\")); }}", items.len()));
            for (idx, n) in items.iter().enumerate() {
                lines.push_str(&format!("{env}.set(\"{n}\", vals[{idx}].clone());"));
            }
            lines.push_str(" } else { return Err(RuntimeError::new(\"expected tuple in for\")); }");
            lines
        }
    };
    Ok(code)
}

fn find_fn_name(name: &str, params: &[String], cg: &Codegen) -> Result<String, RuntimeError> {
    if let Some(r) = cg.fn_map.get(name) {
        return Ok(r.clone());
    }
    Err(RuntimeError::new(format!(
        "function lookup not resolved for {name}/{:?}",
        params
    )))
}
