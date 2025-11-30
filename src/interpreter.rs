use std::collections::{HashMap, HashSet};
use std::io::{Read, SeekFrom, Write};
use std::rc::Rc;
use std::sync::mpsc;

use crate::runtime::{
    BoundMethodValue, ClassValue, Env, FunctionImpl, FunctionValue, HashKey, InstanceValue,
    RuntimeError, Value,
};
use crate::{AssignOp, AssignTarget, BinOp, CmpOp, Expr, ForTarget, Param, Program, Stmt, UnOp};

enum Flow {
    Continue,
    Return(Value),
    Raise(Value),
    Yield(Value),
}

pub struct Interpreter {
    globals: HashMap<String, Value>,
    user_funcs: Vec<UserFunc>,
}

#[derive(Clone)]
struct UserFunc {
    params: Vec<Param>,
    is_generator: bool,
    body: Vec<Stmt>,
}

impl Interpreter {
    pub fn new() -> Self {
        let mut globals = HashMap::new();
        insert_builtins(&mut globals);
        Interpreter {
            globals,
            user_funcs: Vec::new(),
        }
    }

    pub fn run(&mut self, program: &Program) -> Result<(), RuntimeError> {
        let mut env = Env::new();
        self.exec_block(&program.stmts, &mut env, None)?;
        Ok(())
    }

    fn exec_block(
        &mut self,
        stmts: &[Stmt],
        env: &mut Env,
        yield_sender: Option<&mpsc::Sender<Result<Value, RuntimeError>>>,
    ) -> Result<Flow, RuntimeError> {
        for stmt in stmts {
            let flow = match self.exec_stmt(stmt, env, yield_sender) {
                Ok(f) => f,
                Err(e) => return Ok(Flow::Raise(Value::Str(e.message))),
            };
            match flow {
                Flow::Continue => {}
                Flow::Yield(v) => {
                    if let Some(tx) = yield_sender {
                        if tx.send(Ok(v)).is_err() {
                            return Ok(Flow::Return(Value::None));
                        } else {
                            continue;
                        }
                    } else {
                        return Ok(Flow::Yield(v));
                    }
                }
                _ => return Ok(flow),
            }
        }
        Ok(Flow::Continue)
    }

    fn exec_stmt(
        &mut self,
        stmt: &Stmt,
        env: &mut Env,
        yield_sender: Option<&mpsc::Sender<Result<Value, RuntimeError>>>,
    ) -> Result<Flow, RuntimeError> {
        match stmt {
            Stmt::Assign { target, expr, op } => {
                let mut value = self.eval_expr(expr, env)?;
                if *op != AssignOp::Assign {
                    let current = self.eval_assign_target(target, env)?;
                    value = self.apply_assign_op(*op, current, value)?;
                }
                self.store_target(target, value, env)?;
                Ok(Flow::Continue)
            }
            Stmt::Print { args } => {
                let mut parts = Vec::new();
                for a in args {
                    let val = self.eval_expr(a, env)?;
                    parts.push(self.value_to_string(&val));
                }
                println!("{}", parts.join(" "));
                Ok(Flow::Continue)
            }
            Stmt::ExprStmt { expr } => {
                self.eval_expr(expr, env)?;
                Ok(Flow::Continue)
            }
            Stmt::If {
                branches,
                else_branch,
            } => {
                for (cond, body) in branches {
                    if self.eval_expr(cond, env)?.truthy() {
                        return self.exec_block(body, env, yield_sender);
                    }
                }
                if let Some(body) = else_branch {
                    return self.exec_block(body, env, yield_sender);
                }
                Ok(Flow::Continue)
            }
            Stmt::While { cond, body } => {
                while self.eval_expr(cond, env)?.truthy() {
                    let flow = self.exec_block(body, env, yield_sender)?;
                    match flow {
                        Flow::Continue => {}
                        Flow::Return(v) => return Ok(Flow::Return(v)),
                        Flow::Raise(v) => return Ok(Flow::Raise(v)),
                        Flow::Yield(v) => return Ok(Flow::Yield(v)),
                    }
                }
                Ok(Flow::Continue)
            }
            Stmt::For { target, iter, body } => {
                let iterable = self.eval_expr(iter, env)?;
                let mut items = self.iterate_value(iterable)?;
                while let Some(item) = items.next() {
                    let item = item?;
                    self.bind_for_target(target, item, env)?;
                    let flow = self.exec_block(body, env, yield_sender)?;
                    match flow {
                        Flow::Continue => {}
                        Flow::Return(v) => return Ok(Flow::Return(v)),
                        Flow::Raise(v) => return Ok(Flow::Raise(v)),
                        Flow::Yield(v) => return Ok(Flow::Yield(v)),
                    }
                }
                Ok(Flow::Continue)
            }
            Stmt::With { target, expr, body } => {
                let val = self.eval_expr(expr, env)?;
                self.store_target(&AssignTarget::Name(target.clone()), val.clone(), env)?;
                let res = self.exec_block(body, env, yield_sender)?;
                if let Value::File(fv) = val {
                    let _ = fv.file.borrow_mut().flush();
                }
                Ok(res)
            }
            Stmt::Return { expr } => {
                let v = if let Some(e) = expr {
                    self.eval_expr(e, env)?
                } else {
                    Value::None
                };
                Ok(Flow::Return(v))
            }
            Stmt::Function {
                name,
                params,
                is_generator,
                body,
            } => {
                let id = self.user_funcs.len();
                self.user_funcs.push(UserFunc {
                    params: params.clone(),
                    is_generator: *is_generator,
                    body: body.clone(),
                });
                let func = Value::Function(FunctionValue {
                    name: Some(name.clone()),
                    arity: params.len(),
                    impls: FunctionImpl::User(id),
                });
                env.set(name, func);
                Ok(Flow::Continue)
            }
            Stmt::Class { name, methods } => {
                let mut map = HashMap::new();
                for m in methods {
                    if let Stmt::Function {
                        name: mname,
                        params,
                        is_generator,
                        body,
                    } = m
                    {
                        let id = self.user_funcs.len();
                        self.user_funcs.push(UserFunc {
                            params: params.clone(),
                            is_generator: *is_generator,
                            body: body.clone(),
                        });
                        let func = FunctionValue {
                            name: Some(mname.clone()),
                            arity: params.len(),
                            impls: FunctionImpl::User(id),
                        };
                        map.insert(mname.clone(), func);
                    }
                }
                let class = Value::Class(ClassValue {
                    name: name.clone(),
                    methods: map,
                });
                env.set(name, class);
                Ok(Flow::Continue)
            }
            Stmt::Try {
                body,
                handlers,
                finally_body,
            } => {
                let mut result = self.exec_block(body, env, yield_sender)?;
                if let Flow::Raise(ref val) = result {
                    let raised = val.clone();
                    let mut handled = false;
                    for h in handlers {
                        if let Some(ref n) = h.name {
                            if self.match_exception(&raised, n) {
                                let mut new_env = env.child();
                                if let Some(bind) = &h.bind {
                                    new_env.set(bind, raised.clone());
                                }
                                result =
                                    self.exec_block(&h.body, &mut new_env, yield_sender)?;
                                handled = true;
                                break;
                            }
                        } else {
                            let mut new_env = env.child();
                            if let Some(bind) = &h.bind {
                                new_env.set(bind, raised.clone());
                            }
                            result = self.exec_block(&h.body, &mut new_env, yield_sender)?;
                            handled = true;
                            break;
                        }
                    }
                    if !handled {
                        result = Flow::Raise(raised);
                    }
                }
                if let Some(fbody) = finally_body {
                    let flow = self.exec_block(fbody, env, yield_sender)?;
                    match flow {
                        Flow::Continue => {}
                        _ => return Ok(flow),
                    }
                }
                Ok(result)
            }
            Stmt::Raise { expr } => {
                let v = self.eval_expr(expr, env)?;
                Ok(Flow::Raise(v))
            }
            Stmt::Yield { expr } => {
                let v = if let Some(e) = expr {
                    self.eval_expr(e, env)?
                } else {
                    Value::None
                };
                Ok(Flow::Yield(v))
            }
        }
    }

    fn eval_assign_target(
        &mut self,
        target: &AssignTarget,
        env: &mut Env,
    ) -> Result<Value, RuntimeError> {
        match target {
            AssignTarget::Name(n) => env
                .get(n)
                .ok_or_else(|| RuntimeError::new("Name not found")),
            AssignTarget::Subscript { object, index } => {
                let obj = self.eval_expr(object, env)?;
                let idx = self.eval_expr(index, env)?;
                self.subscript_get(obj, idx)
            }
            AssignTarget::Attr { object, attr } => {
                let obj = self.eval_expr(object, env)?;
                self.attr_get(obj, attr)
            }
            AssignTarget::Tuple { items } => {
                let mut vals = Vec::new();
                for it in items {
                    vals.push(self.eval_assign_target(it, env)?);
                }
                Ok(Value::Tuple(vals))
            }
            AssignTarget::Starred(_) => Err(RuntimeError::new("starred get not supported")),
        }
    }

    fn store_target(
        &mut self,
        target: &AssignTarget,
        value: Value,
        env: &mut Env,
    ) -> Result<(), RuntimeError> {
        match target {
            AssignTarget::Name(n) => {
                env.set(n, value);
                Ok(())
            }
            AssignTarget::Subscript { object, index } => {
                let obj = self.eval_expr(object, env)?;
                let idx = self.eval_expr(index, env)?;
                self.subscript_set(obj, idx, value)
            }
            AssignTarget::Attr { object, attr } => {
                let obj = self.eval_expr(object, env)?;
                self.attr_set(obj, attr, value)
            }
            AssignTarget::Tuple { items } => {
                let seq = match value {
                    Value::Tuple(v) => v,
                    Value::List(v) => v.borrow().clone(),
                    _ => return Err(RuntimeError::new("cannot unpack non-sequence")),
                };
                let star_pos = items
                    .iter()
                    .position(|t| matches!(t, AssignTarget::Starred(_)));
                if let Some(pos) = star_pos {
                    if seq.len() + 1 < items.len() {
                        return Err(RuntimeError::new("unpack mismatch"));
                    }
                    let before = &items[..pos];
                    let after = &items[pos + 1..];
                    for (t, v) in before.iter().zip(seq.iter().take(before.len())) {
                        self.store_target(t, v.clone(), env)?;
                    }
                    let rest_count = seq.len() - before.len() - after.len();
                    let rest_vals = seq
                        .iter()
                        .skip(before.len())
                        .take(rest_count)
                        .cloned()
                        .collect::<Vec<_>>();
                    if let AssignTarget::Starred(name) = &items[pos] {
                        self.store_target(
                            &AssignTarget::Name(name.clone()),
                            Value::List(Rc::new(std::cell::RefCell::new(rest_vals))),
                            env,
                        )?;
                    }
                    for (t, v) in after.iter().zip(seq.iter().rev().take(after.len()).rev()) {
                        self.store_target(t, v.clone(), env)?;
                    }
                    Ok(())
                } else {
                    if items.len() != seq.len() {
                        return Err(RuntimeError::new("unpack mismatch"));
                    }
                    for (t, v) in items.iter().zip(seq.into_iter()) {
                        self.store_target(t, v, env)?;
                    }
                    Ok(())
                }
            }
            AssignTarget::Starred(name) => {
                env.set(name, value);
                Ok(())
            }
        }
    }

    fn apply_assign_op(&self, op: AssignOp, lhs: Value, rhs: Value) -> Result<Value, RuntimeError> {
        match op {
            AssignOp::Assign => Ok(rhs),
            AssignOp::Add => self.binary(lhs, rhs, BinOp::Add),
            AssignOp::Sub => self.binary(lhs, rhs, BinOp::Sub),
            AssignOp::Mul => self.binary(lhs, rhs, BinOp::Mul),
            AssignOp::Div => self.binary(lhs, rhs, BinOp::Div),
        }
    }

    fn eval_expr(&mut self, expr: &Expr, env: &mut Env) -> Result<Value, RuntimeError> {
        match expr {
            Expr::Int(i) => Ok(Value::Int(*i)),
            Expr::Float(f) => Ok(Value::Float(*f)),
            Expr::Str(s) => Ok(Value::Str(s.clone())),
            Expr::Bool(b) => Ok(Value::Bool(*b)),
            Expr::Var(name) => env
                .get(name)
                .or_else(|| self.globals.get(name).cloned())
                .ok_or_else(|| RuntimeError::new(format!("Name not found: {name}"))),
            Expr::Lambda { params, body } => {
                let id = self.user_funcs.len();
                let param_defs: Vec<Param> = params
                    .iter()
                    .map(|p| Param {
                        name: p.clone(),
                        default: None,
                    })
                    .collect();
                self.user_funcs.push(UserFunc {
                    params: param_defs,
                    is_generator: false,
                    body: vec![Stmt::Return {
                        expr: Some((**body).clone()),
                    }],
                });
                Ok(Value::Function(FunctionValue {
                    name: None,
                    arity: params.len(),
                    impls: FunctionImpl::User(id),
                }))
            }
            Expr::Unary(op, e) => {
                let v = self.eval_expr(e, env)?;
                match op {
                    UnOp::Neg => self.negate(v),
                    UnOp::Not => Ok(Value::Bool(!v.truthy())),
                }
            }
            Expr::Binary(l, op, r) => {
                let lv = self.eval_expr(l, env)?;
                let rv = self.eval_expr(r, env)?;
                self.binary(lv, rv, *op)
            }
            Expr::Compare(l, op, r) => {
                let lv = self.eval_expr(l, env)?;
                let rv = self.eval_expr(r, env)?;
                self.compare(lv, rv, *op)
            }
            Expr::Call {
                callee,
                args,
                kwargs,
            } => {
                let cal = self.eval_expr(callee, env)?;
                let mut argv = Vec::new();
                for a in args {
                    argv.push(self.eval_expr(a, env)?);
                }
                let mut kw_vals = Vec::new();
                for (k, v) in kwargs {
                    kw_vals.push((k.clone(), self.eval_expr(v, env)?));
                }
                self.call_value(cal, argv, kw_vals, env)
            }
            Expr::List(items) => {
                let mut vals = Vec::new();
                for i in items {
                    vals.push(self.eval_expr(i, env)?);
                }
                Ok(Value::List(Rc::new(std::cell::RefCell::new(vals))))
            }
            Expr::Tuple(items) => {
                let mut vals = Vec::new();
                for i in items {
                    vals.push(self.eval_expr(i, env)?);
                }
                Ok(Value::Tuple(vals))
            }
            Expr::Dict(entries) => {
                let mut map = HashMap::new();
                for (k, v) in entries {
                    let kv = self.eval_expr(k, env)?;
                    let vv = self.eval_expr(v, env)?;
                    let key = HashKey::from_value(&kv)
                        .ok_or_else(|| RuntimeError::new("unhashable dict key"))?;
                    map.insert(key, vv);
                }
                Ok(Value::Dict(Rc::new(std::cell::RefCell::new(map))))
            }
            Expr::Set(items) => {
                let mut set = HashSet::new();
                for i in items {
                    let v = self.eval_expr(i, env)?;
                    let key = HashKey::from_value(&v)
                        .ok_or_else(|| RuntimeError::new("unhashable set"))?;
                    set.insert(key);
                }
                Ok(Value::Set(Rc::new(std::cell::RefCell::new(set))))
            }
            Expr::Index(obj, idx) => {
                let o = self.eval_expr(obj, env)?;
                let i = self.eval_expr(idx, env)?;
                self.subscript_get(o, i)
            }
            Expr::Slice { value, start, end } => {
                let v = self.eval_expr(value, env)?;
                let s = if let Some(sv) = start {
                    Some(self.eval_expr(sv, env)?)
                } else {
                    None
                };
                let e = if let Some(ev) = end {
                    Some(self.eval_expr(ev, env)?)
                } else {
                    None
                };
                self.slice(v, s, e)
            }
            Expr::MethodCall {
                object,
                method,
                args,
                kwargs,
            } => {
                let obj = self.eval_expr(object, env)?;
                let callee = self.attr_get(obj, method)?;
                let mut args_vals = Vec::new();
                for a in args {
                    args_vals.push(self.eval_expr(a, env)?);
                }
                let mut kw_vals = Vec::new();
                for (k, v) in kwargs {
                    kw_vals.push((k.clone(), self.eval_expr(v, env)?));
                }
                self.call_value(callee, args_vals, kw_vals, env)
            }
            Expr::Attr { object, attr } => {
                let obj = self.eval_expr(object, env)?;
                self.attr_get(obj, attr)
            }
            Expr::ListComp {
                target,
                iter,
                expr,
                cond,
            } => {
                let iterable = self.eval_expr(iter, env)?;
                let mut items = self.iterate_value(iterable)?;
                let mut result = Vec::new();
                while let Some(item) = items.next() {
                    let item = item?;
                    let mut child = env.child();
                    self.bind_for_target(target, item, &mut child)?;
                    if let Some(c) = cond {
                        if !self.eval_expr(c, &mut child)?.truthy() {
                            continue;
                        }
                    }
                    result.push(self.eval_expr(expr, &mut child)?);
                }
                Ok(Value::List(Rc::new(std::cell::RefCell::new(result))))
            }
            Expr::DictComp {
                target,
                iter,
                key,
                value,
                cond,
            } => {
                let iterable = self.eval_expr(iter, env)?;
                let mut items = self.iterate_value(iterable)?;
                let mut map = HashMap::new();
                while let Some(item) = items.next() {
                    let item = item?;
                    let mut child = env.child();
                    self.bind_for_target(target, item, &mut child)?;
                    if let Some(c) = cond {
                        if !self.eval_expr(c, &mut child)?.truthy() {
                            continue;
                        }
                    }
                    let k = self.eval_expr(key, &mut child)?;
                    let v = self.eval_expr(value, &mut child)?;
                    let hk = HashKey::from_value(&k)
                        .ok_or_else(|| RuntimeError::new("unhashable key"))?;
                    map.insert(hk, v);
                }
                Ok(Value::Dict(Rc::new(std::cell::RefCell::new(map))))
            }
        }
    }

    fn bind_for_target(
        &mut self,
        target: &ForTarget,
        value: Value,
        env: &mut Env,
    ) -> Result<(), RuntimeError> {
        match target {
            ForTarget::Name(n) => {
                env.set(n, value);
                Ok(())
            }
            ForTarget::Tuple(names) => match value {
                Value::Tuple(vals) => {
                    if vals.len() != names.len() {
                        return Err(RuntimeError::new("unpack mismatch"));
                    }
                    for (n, v) in names.iter().zip(vals.into_iter()) {
                        env.set(n, v);
                    }
                    Ok(())
                }
                Value::List(vs) => {
                    let vals = vs.borrow().clone();
                    if vals.len() != names.len() {
                        return Err(RuntimeError::new("unpack mismatch"));
                    }
                    for (n, v) in names.iter().zip(vals.into_iter()) {
                        env.set(n, v);
                    }
                    Ok(())
                }
                _ => Err(RuntimeError::new("cannot unpack iter target")),
            },
        }
    }

    fn negate(&self, v: Value) -> Result<Value, RuntimeError> {
        match v {
            Value::Int(i) => Ok(Value::Int(-i)),
            Value::Float(f) => Ok(Value::Float(-f)),
            _ => Err(RuntimeError::new("bad operand for unary -")),
        }
    }

    fn binary(&self, lhs: Value, rhs: Value, op: BinOp) -> Result<Value, RuntimeError> {
        match op {
            BinOp::Add => match (lhs, rhs) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
                (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 + b)),
                (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a + b as f64)),
                (Value::Str(a), Value::Str(b)) => Ok(Value::Str(format!("{a}{b}"))),
                (Value::List(a), Value::List(b)) => {
                    let mut merged = a.borrow().clone();
                    merged.extend(b.borrow().iter().cloned());
                    Ok(Value::List(Rc::new(std::cell::RefCell::new(merged))))
                }
                _ => Err(RuntimeError::new("unsupported +")),
            },
            BinOp::Sub => match (lhs, rhs) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a - b)),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
                (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 - b)),
                (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a - b as f64)),
                _ => Err(RuntimeError::new("unsupported -")),
            },
            BinOp::Mul => match (lhs, rhs) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a * b)),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
                (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 * b)),
                (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a * b as f64)),
                _ => Err(RuntimeError::new("unsupported *")),
            },
            BinOp::Div => match (lhs, rhs) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Float(a as f64 / b as f64)),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a / b)),
                (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 / b)),
                (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a / b as f64)),
                _ => Err(RuntimeError::new("unsupported /")),
            },
        }
    }

    fn compare(&self, lhs: Value, rhs: Value, op: CmpOp) -> Result<Value, RuntimeError> {
        let result = match op {
            CmpOp::Eq => self.value_eq(&lhs, &rhs)?,
            CmpOp::Ne => !self.value_eq(&lhs, &rhs)?,
            CmpOp::Lt => self.ordered(&lhs, &rhs, |o| o == std::cmp::Ordering::Less)?,
            CmpOp::Le => self.ordered(&lhs, &rhs, |o| {
                o == std::cmp::Ordering::Less || o == std::cmp::Ordering::Equal
            })?,
            CmpOp::Gt => self.ordered(&lhs, &rhs, |o| o == std::cmp::Ordering::Greater)?,
            CmpOp::Ge => self.ordered(&lhs, &rhs, |o| {
                o == std::cmp::Ordering::Greater || o == std::cmp::Ordering::Equal
            })?,
        };
        Ok(Value::Bool(result))
    }

    fn value_eq(&self, a: &Value, b: &Value) -> Result<bool, RuntimeError> {
        Ok(match (a, b) {
            (Value::Int(x), Value::Int(y)) => x == y,
            (Value::Float(x), Value::Float(y)) => x == y,
            (Value::Bool(x), Value::Bool(y)) => x == y,
            (Value::Str(x), Value::Str(y)) => x == y,
            _ => false,
        })
    }

    fn ordered(
        &self,
        a: &Value,
        b: &Value,
        is_ok: fn(std::cmp::Ordering) -> bool,
    ) -> Result<bool, RuntimeError> {
        match (a, b) {
            (Value::Int(x), Value::Int(y)) => Ok(is_ok(x.cmp(y))),
            (Value::Float(x), Value::Float(y)) => Ok(is_ok(
                x.partial_cmp(y)
                    .ok_or_else(|| RuntimeError::new("NaN compare"))?,
            )),
            (Value::Int(x), Value::Float(y)) => Ok(is_ok(
                (*x as f64)
                    .partial_cmp(y)
                    .ok_or_else(|| RuntimeError::new("NaN compare"))?,
            )),
            (Value::Float(x), Value::Int(y)) => Ok(is_ok(
                x.partial_cmp(&(*y as f64))
                    .ok_or_else(|| RuntimeError::new("NaN compare"))?,
            )),
            (Value::Str(x), Value::Str(y)) => Ok(is_ok(x.cmp(y))),
            _ => Err(RuntimeError::new("unorderable types")),
        }
    }

    fn call_value(
        &mut self,
        callee: Value,
        mut args: Vec<Value>,
        kwargs: Vec<(String, Value)>,
        env: &mut Env,
    ) -> Result<Value, RuntimeError> {
        match callee {
            Value::Function(f) => match f.impls {
                FunctionImpl::Native(func) => {
                    if f.name.as_deref() == Some("sorted") {
                        return self.builtin_sorted(args, env);
                    }
                    func(&args, &kwargs, env, &mut self.globals)
                }
                FunctionImpl::User(id) => {
                    let def = self
                        .user_funcs
                        .get(id)
                        .cloned()
                        .ok_or_else(|| RuntimeError::new("unknown function"))?;
                    if args.len() > def.params.len() {
                        return Err(RuntimeError::new("too many args"));
                    }
                    let mut child = self.bind_user_call(&def, args, kwargs, env)?;
                    if def.is_generator {
                        let mut outputs = Vec::new();
                        loop {
                            match self.exec_block(&def.body, &mut child, None)? {
                                Flow::Yield(v) => outputs.push(v),
                                Flow::Return(_) | Flow::Continue => break,
                                Flow::Raise(v) => {
                                    return Err(RuntimeError::new(self.value_to_string(&v)))
                                }
                            }
                        }
                        return Ok(Value::List(Rc::new(std::cell::RefCell::new(outputs))));
                    }
                    match self.exec_block(&def.body, &mut child, None)? {
                        Flow::Return(v) => Ok(v),
                        Flow::Raise(v) => Err(RuntimeError::new(self.value_to_string(&v))),
                        Flow::Continue => Ok(Value::None),
                        Flow::Yield(_) => Err(RuntimeError::new("yield outside generator call")),
                    }
                }
            },
            Value::BoundMethod(b) => {
                args.insert(0, b.receiver.clone());
                self.call_value(Value::Function(b.func), args, kwargs, env)
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
                    self.call_value(Value::BoundMethod(Box::new(bm)), args, kwargs, env)?;
                }
                Ok(inst)
            }
            _ => Err(RuntimeError::new("cannot call value")),
        }
    }

    fn bind_user_call(
        &mut self,
        def: &UserFunc,
        args: Vec<Value>,
        kwargs: Vec<(String, Value)>,
        env: &mut Env,
    ) -> Result<Env, RuntimeError> {
        let mut kw_map = HashMap::new();
        for (k, v) in kwargs.into_iter() {
            if kw_map.insert(k.clone(), v).is_some() {
                return Err(RuntimeError::new("duplicate keyword arg"));
            }
        }
        let mut child = env.child();
        for (idx, param) in def.params.iter().enumerate() {
            if let Some(v) = args.get(idx) {
                child.set(&param.name, v.clone());
                continue;
            }
            if let Some(v) = kw_map.remove(&param.name) {
                child.set(&param.name, v);
                continue;
            }
            if let Some(def_expr) = &param.default {
                let v = self.eval_expr(def_expr, env)?;
                child.set(&param.name, v);
                continue;
            }
            return Err(RuntimeError::new(format!("missing argument {}", param.name)));
        }
        if !kw_map.is_empty() {
            return Err(RuntimeError::new("unexpected keyword arguments"));
        }
        Ok(child)
    }

    fn iterate_value(&self, v: Value) -> Result<ValueIterator, RuntimeError> {
        match v {
            Value::List(items) => Ok(ValueIterator::from_vec(items.borrow().clone())),
            Value::Tuple(items) => Ok(ValueIterator::from_vec(items)),
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
                Ok(ValueIterator::from_vec(res))
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
                Ok(ValueIterator::from_vec(vals))
            }
            Value::Str(s) => Ok(ValueIterator::from_vec(
                s.chars().map(|c| Value::Str(c.to_string())).collect(),
            )),
            Value::Generator(g) => Ok(ValueIterator::from_generator(g.rx.clone())),
            _ => Err(RuntimeError::new("not iterable")),
        }
    }

    fn subscript_get(&self, obj: Value, idx: Value) -> Result<Value, RuntimeError> {
        match obj {
            Value::List(items) => match idx {
                Value::Int(i) => {
                    let vec = items.borrow();
                    let ii = self.fix_index(i, vec.len())?;
                    Ok(vec[ii].clone())
                }
                _ => Err(RuntimeError::new("list index must be int")),
            },
            Value::Tuple(items) => match idx {
                Value::Int(i) => {
                    let ii = self.fix_index(i, items.len())?;
                    Ok(items[ii].clone())
                }
                _ => Err(RuntimeError::new("tuple index must be int")),
            },
            Value::Dict(map) => {
                let key =
                    HashKey::from_value(&idx).ok_or_else(|| RuntimeError::new("unhashable key"))?;
                map.borrow()
                    .get(&key)
                    .cloned()
                    .ok_or_else(|| RuntimeError::new("key not found"))
            }
            Value::Str(s) => match idx {
                Value::Int(i) => {
                    let chars: Vec<_> = s.chars().collect();
                    let ii = self.fix_index(i, chars.len())?;
                    Ok(Value::Str(chars[ii].to_string()))
                }
                _ => Err(RuntimeError::new("str index must be int")),
            },
            _ => Err(RuntimeError::new("not subscriptable")),
        }
    }

    fn subscript_set(&self, obj: Value, idx: Value, value: Value) -> Result<(), RuntimeError> {
        match obj {
            Value::List(items) => match idx {
                Value::Int(i) => {
                    let mut vec = items.borrow_mut();
                    let ii = self.fix_index(i, vec.len())?;
                    vec[ii] = value;
                    Ok(())
                }
                _ => Err(RuntimeError::new("list index must be int")),
            },
            Value::Dict(map) => {
                let key =
                    HashKey::from_value(&idx).ok_or_else(|| RuntimeError::new("unhashable key"))?;
                map.borrow_mut().insert(key, value);
                Ok(())
            }
            _ => Err(RuntimeError::new("item assignment not supported")),
        }
    }

    fn slice(
        &self,
        value: Value,
        start: Option<Value>,
        end: Option<Value>,
    ) -> Result<Value, RuntimeError> {
        let (sidx, eidx, len) = match &value {
            Value::List(items) => {
                let len = items.borrow().len();
                (start, end, len)
            }
            Value::Str(s) => (start, end, s.chars().count()),
            _ => return Err(RuntimeError::new("slice on unsupported type")),
        };
        let s = if let Some(Value::Int(i)) = sidx {
            self.clamp_index(i, len)
        } else {
            0
        };
        let e = if let Some(Value::Int(i)) = eidx {
            self.clamp_index(i, len)
        } else {
            len
        };
        match value {
            Value::List(items) => {
                let vec = items.borrow();
                Ok(Value::List(Rc::new(std::cell::RefCell::new(
                    vec[s..e].to_vec(),
                ))))
            }
            Value::Str(sval) => {
                let chars: Vec<_> = sval.chars().collect();
                let slice: String = chars[s..e].iter().collect();
                Ok(Value::Str(slice))
            }
            _ => Err(RuntimeError::new("slice unsupported")),
        }
    }

    fn attr_get(&self, obj: Value, attr: &str) -> Result<Value, RuntimeError> {
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
                        Ok(Value::List(Rc::new(std::cell::RefCell::new(vec))))
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
                        Ok(Value::List(Rc::new(std::cell::RefCell::new(vec))))
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
                        Ok(Value::Set(Rc::new(std::cell::RefCell::new(s))))
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
                        Ok(Value::List(Rc::new(std::cell::RefCell::new(parts))))
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
                        Ok(Value::Bool(s.starts_with(pref)))
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
                        Ok(Value::Bool(s.ends_with(suf)))
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
                        Ok(Value::List(Rc::new(std::cell::RefCell::new(vals))))
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
                        Ok(Value::List(Rc::new(std::cell::RefCell::new(vals))))
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

    fn attr_set(&self, obj: Value, attr: &str, value: Value) -> Result<(), RuntimeError> {
        match obj {
            Value::Instance(inst) => {
                inst.fields.borrow_mut().insert(attr.to_string(), value);
                Ok(())
            }
            _ => Err(RuntimeError::new("attribute assignment not supported")),
        }
    }

    fn match_exception(&self, raised: &Value, name: &str) -> bool {
        match raised {
            Value::Str(s) => s == name,
            _ => false,
        }
    }

    fn fix_index(&self, idx: i64, len: usize) -> Result<usize, RuntimeError> {
        let l = len as i64;
        let mut i = idx;
        if i < 0 {
            i = l + i;
        }
        if i < 0 || i >= l {
            return Err(RuntimeError::new("index out of range"));
        }
        Ok(i as usize)
    }

    fn clamp_index(&self, idx: i64, len: usize) -> usize {
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

    fn value_to_string(&self, v: &Value) -> String {
        value_to_string(v)
    }

    fn builtin_sorted(&mut self, args: Vec<Value>, env: &mut Env) -> Result<Value, RuntimeError> {
        if args.is_empty() || args.len() > 2 {
            return Err(RuntimeError::new("sorted expects 1 or 2 args"));
        }
        let iterable = args[0].clone();
        let key_fn = if args.len() == 2 {
            Some(args[1].clone())
        } else {
            None
        };
        let mut items = match iterable {
            Value::List(v) => v.borrow().clone(),
            Value::Tuple(v) => v,
            _ => return Err(RuntimeError::new("sorted expects list/tuple")),
        };
        items.sort_by(|a, b| {
            let ka = key_fn
                .as_ref()
                .map(|f| self.call_value(f.clone(), vec![a.clone()], Vec::new(), env))
                .unwrap_or(Ok(a.clone()));
            let kb = key_fn
                .as_ref()
                .map(|f| self.call_value(f.clone(), vec![b.clone()], Vec::new(), env))
                .unwrap_or(Ok(b.clone()));
            match (ka, kb) {
                (Ok(va), Ok(vb)) => compare_for_sort(&va, &vb),
                _ => std::cmp::Ordering::Equal,
            }
        });
        Ok(Value::List(Rc::new(std::cell::RefCell::new(items))))
    }
}

pub fn value_to_string(v: &Value) -> String {
    match v {
        Value::None => "None".into(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Str(s) => s.clone(),
        Value::List(items) => {
            let vals: Vec<String> = items.borrow().iter().map(|v| value_to_string(v)).collect();
            format!("[{}]", vals.join(", "))
        }
        Value::Tuple(items) => {
            let vals: Vec<String> = items.iter().map(|v| value_to_string(v)).collect();
            format!("({})", vals.join(", "))
        }
        Value::Dict(map) => {
            let mut vals: Vec<(String, String)> = map
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
            vals.sort_by(|a, b| a.0.cmp(&b.0));
            format!(
                "{{{}}}",
                vals.into_iter()
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

struct ValueIterator {
    kind: ValueIterKind,
}

enum ValueIterKind {
    Vec(std::vec::IntoIter<Value>),
    Generator(Rc<mpsc::Receiver<Result<Value, RuntimeError>>>),
}

impl ValueIterator {
    fn from_vec(v: Vec<Value>) -> Self {
        ValueIterator {
            kind: ValueIterKind::Vec(v.into_iter()),
        }
    }

    fn from_generator(rx: Rc<mpsc::Receiver<Result<Value, RuntimeError>>>) -> Self {
        ValueIterator {
            kind: ValueIterKind::Generator(rx),
        }
    }

    fn next(&mut self) -> Option<Result<Value, RuntimeError>> {
        match &mut self.kind {
            ValueIterKind::Vec(iter) => iter.next().map(Ok),
            ValueIterKind::Generator(rx) => rx.recv().ok(),
        }
    }
}

fn insert_builtins(globals: &mut HashMap<String, Value>) {
    globals.insert(
        "int".into(),
        Value::Function(FunctionValue {
            name: Some("int".into()),
            arity: 1,
            impls: FunctionImpl::Native(Rc::new(|args, _kwargs, _env, _globals| match &args[0] {
                Value::Int(i) => Ok(Value::Int(*i)),
                Value::Float(f) => Ok(Value::Int(*f as i64)),
                Value::Bool(b) => Ok(Value::Int(if *b { 1 } else { 0 })),
                Value::Str(s) => s
                    .parse::<i64>()
                    .map(Value::Int)
                    .map_err(|e| RuntimeError::new(e.to_string())),
                _ => Err(RuntimeError::new("cannot convert to int")),
            })),
        }),
    );
    globals.insert(
        "len".into(),
        Value::Function(FunctionValue {
            name: Some("len".into()),
            arity: 1,
            impls: FunctionImpl::Native(Rc::new(|args, _kwargs, _env, _globals| {
                let v = &args[0];
                let len = match v {
                    Value::List(items) => items.borrow().len(),
                    Value::Tuple(items) => items.len(),
                    Value::Dict(map) => map.borrow().len(),
                    Value::Set(set) => set.borrow().len(),
                    Value::Str(s) => s.chars().count(),
                    _ => return Err(RuntimeError::new("len expects list/tuple/dict/set/str")),
                };
                Ok(Value::Int(len as i64))
            })),
        }),
    );
    globals.insert(
        "sleep".into(),
        Value::Function(FunctionValue {
            name: Some("sleep".into()),
            arity: 1,
            impls: FunctionImpl::Native(Rc::new(|args, _kwargs, _env, _globals| {
                let secs = match &args[0] {
                    Value::Int(i) => *i as f64,
                    Value::Float(f) => *f,
                    _ => return Err(RuntimeError::new("sleep expects int/float seconds")),
                };
                if secs > 0.0 {
                    let dur = std::time::Duration::from_secs_f64(secs);
                    std::thread::sleep(dur);
                }
                Ok(Value::None)
            })),
        }),
    );
    globals.insert(
        "time".into(),
        Value::Function(FunctionValue {
            name: Some("time".into()),
            arity: 0,
            impls: FunctionImpl::Native(Rc::new(|_args, _kwargs, _env, _globals| {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|e| RuntimeError::new(e.to_string()))?;
                Ok(Value::Float(now.as_secs_f64()))
            })),
        }),
    );
    globals.insert(
        "interrupt".into(),
        Value::Function(FunctionValue {
            name: Some("interrupt".into()),
            arity: 0,
            impls: FunctionImpl::Native(Rc::new(|_args, _kwargs, _env, _globals| {
                Err(RuntimeError::new("KeyboardInterrupt"))
            })),
        }),
    );
    globals.insert(
        "requests".into(),
        crate::runtime::http::build_requests_module(),
    );
    globals.insert(
        "float".into(),
        Value::Function(FunctionValue {
            name: Some("float".into()),
            arity: 1,
            impls: FunctionImpl::Native(Rc::new(|args, _kwargs, _env, _globals| match &args[0] {
                Value::Int(i) => Ok(Value::Float(*i as f64)),
                Value::Float(f) => Ok(Value::Float(*f)),
                Value::Bool(b) => Ok(Value::Float(if *b { 1.0 } else { 0.0 })),
                Value::Str(s) => s
                    .parse::<f64>()
                    .map(Value::Float)
                    .map_err(|e| RuntimeError::new(e.to_string())),
                _ => Err(RuntimeError::new("cannot convert to float")),
            })),
        }),
    );
    globals.insert(
        "str".into(),
        Value::Function(FunctionValue {
            name: Some("str".into()),
            arity: 1,
            impls: FunctionImpl::Native(Rc::new(|args, _kwargs, _env, _globals| {
                Ok(Value::Str(value_to_string(&args[0])))
            })),
        }),
    );
    globals.insert(
        "range".into(),
        Value::Function(FunctionValue {
            name: Some("range".into()),
            arity: 3, // we will allow 1-3 by inspecting args len
            impls: FunctionImpl::Native(Rc::new(|args, _kwargs, _env, _globals| {
                let len = args.len();
                let (start, end, step) = match len {
                    1 => (0, as_int(&args[0])?, 1),
                    2 => (as_int(&args[0])?, as_int(&args[1])?, 1),
                    3 => (as_int(&args[0])?, as_int(&args[1])?, as_int(&args[2])?),
                    _ => return Err(RuntimeError::new("range expects 1-3 args")),
                };
                if step == 0 {
                    return Err(RuntimeError::new("range step cannot be 0"));
                }
                let mut vals = Vec::new();
                let mut i = start;
                if step > 0 {
                    while i < end {
                        vals.push(Value::Int(i));
                        i += step;
                    }
                } else {
                    while i > end {
                        vals.push(Value::Int(i));
                        i += step;
                    }
                }
                Ok(Value::List(Rc::new(std::cell::RefCell::new(vals))))
            })),
        }),
    );
    globals.insert(
        "enumerate".into(),
        Value::Function(FunctionValue {
            name: Some("enumerate".into()),
            arity: 2, // allow 1-2
            impls: FunctionImpl::Native(Rc::new(|args, _kwargs, _env, _globals| {
                let start = if args.len() == 2 {
                    as_int(&args[1])?
                } else {
                    0
                };
                let items = match &args[0] {
                    Value::List(v) => v.borrow().clone(),
                    Value::Tuple(v) => v.clone(),
                    _ => return Err(RuntimeError::new("enumerate expects list/tuple")),
                };
                let mut res = Vec::new();
                for (idx, item) in items.into_iter().enumerate() {
                    res.push(Value::Tuple(vec![Value::Int(start + idx as i64), item]));
                }
                Ok(Value::List(Rc::new(std::cell::RefCell::new(res))))
            })),
        }),
    );
    globals.insert(
        "sorted".into(),
        Value::Function(FunctionValue {
            name: Some("sorted".into()),
            arity: 2, // allow 1-2
            impls: FunctionImpl::Native(Rc::new(|args, _kwargs, _env, _globals| {
                let (iterable, key_fn) = match args.len() {
                    1 => (&args[0], None),
                    2 => (&args[0], Some(args[1].clone())),
                    _ => return Err(RuntimeError::new("sorted expects 1 or 2 args")),
                };
                let mut items = match iterable {
                    Value::List(v) => v.borrow().clone(),
                    Value::Tuple(v) => v.clone(),
                    _ => return Err(RuntimeError::new("sorted expects list/tuple")),
                };
                items.sort_by(|a, b| {
                    let ka = key_fn.as_ref().map(|f| {
                        call_simple(
                            f.clone(),
                            vec![a.clone()],
                            &mut Env::new(),
                            &mut HashMap::new(),
                        )
                    }).unwrap_or(Ok(a.clone()));
                    let kb = key_fn.as_ref().map(|f| {
                        call_simple(
                            f.clone(),
                            vec![b.clone()],
                            &mut Env::new(),
                            &mut HashMap::new(),
                        )
                    }).unwrap_or(Ok(b.clone()));
                    match (ka, kb) {
                        (Ok(va), Ok(vb)) => compare_for_sort(&va, &vb),
                        _ => std::cmp::Ordering::Equal,
                    }
                });
                Ok(Value::List(Rc::new(std::cell::RefCell::new(items))))
            })),
        }),
    );
    globals.insert(
        "open".into(),
        Value::Function(FunctionValue {
            name: Some("open".into()),
            arity: 2, // allow 1-2
            impls: FunctionImpl::Native(Rc::new(|args, _kwargs, _env, _globals| {
                use std::fs::OpenOptions;
                let path = match &args[0] {
                    Value::Str(s) => s.clone(),
                    _ => return Err(RuntimeError::new("open expects path string")),
                };
                let mode = if args.len() == 2 {
                    match &args[1] {
                        Value::Str(s) => s.clone(),
                        _ => return Err(RuntimeError::new("mode must be string")),
                    }
                } else {
                    "r".into()
                };
                let mut opts = OpenOptions::new();
                match mode.as_str() {
                    "r" => {
                        opts.read(true);
                    }
                    "w" => {
                        opts.write(true).create(true).truncate(true);
                    }
                    "a" => {
                        opts.append(true).create(true);
                    }
                    _ => return Err(RuntimeError::new("unsupported mode")),
                }
                let file = opts
                    .open(&path)
                    .map_err(|e| RuntimeError::new(e.to_string()))?;
                Ok(Value::File(crate::runtime::FileValue {
                    path,
                    mode,
                    file: Rc::new(std::cell::RefCell::new(file)),
                }))
            })),
        }),
    );
    globals.insert("SEEK_SET".into(), Value::Int(crate::runtime::SEEK_SET));
    globals.insert("SEEK_CUR".into(), Value::Int(crate::runtime::SEEK_CUR));
    globals.insert("SEEK_END".into(), Value::Int(crate::runtime::SEEK_END));
    globals.insert(
        "KeyboardInterrupt".into(),
        Value::Str("KeyboardInterrupt".into()),
    );
}

fn as_int(v: &Value) -> Result<i64, RuntimeError> {
    match v {
        Value::Int(i) => Ok(*i),
        _ => Err(RuntimeError::new("expected int")),
    }
}

fn call_simple(
    func: Value,
    args: Vec<Value>,
    env: &mut Env,
    globals: &mut HashMap<String, Value>,
) -> Result<Value, RuntimeError> {
    match func {
        Value::Function(f) => match f.impls {
            FunctionImpl::Native(n) => n(&args, &[], env, globals),
            _ => Err(RuntimeError::new("key func must be native here")),
        },
        Value::BoundMethod(b) => call_simple(Value::Function(b.func), {
            let mut a = vec![b.receiver.clone()];
            a.extend(args);
            a
        }, env, globals),
        _ => Err(RuntimeError::new("not callable")),
    }
}

fn compare_for_sort(a: &Value, b: &Value) -> std::cmp::Ordering {
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

fn values_equal(a: &Value, b: &Value) -> Result<bool, RuntimeError> {
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => Ok(x == y),
        (Value::Float(x), Value::Float(y)) => Ok(x == y),
        (Value::Bool(x), Value::Bool(y)) => Ok(x == y),
        (Value::Str(x), Value::Str(y)) => Ok(x == y),
        _ => Ok(false),
    }
}

pub fn builtins() -> HashMap<String, Value> {
    let mut globals = HashMap::new();
    insert_builtins(&mut globals);
    globals
}
