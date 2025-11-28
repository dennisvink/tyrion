use std::collections::HashSet;
use std::env;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{self, Command};
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} <input.ty> <output_executable>", args[0]);
        process::exit(1);
    }

    let input_path = PathBuf::from(&args[1]);
    let output_path = PathBuf::from(&args[2]);
    if !input_path.exists() {
        return Err(format!("Input file not found: {}", input_path.display()).into());
    }

    let source = fs::read_to_string(&input_path)?;
    let program = parse_program(&source)?;
    let c_source = codegen_c(&program);
    compile_c_to_exe(&c_source, &output_path)?;

    println!("Compiled {} -> {}", input_path.display(), output_path.display());
    Ok(())
}

#[derive(Debug, Clone)]
struct Program {
    stmts: Vec<Stmt>,
}

#[derive(Debug, Clone)]
enum Stmt {
    Function { name: String, params: Vec<String>, body: Vec<Stmt> },
    Class { name: String, methods: Vec<Stmt> },
    Assign { target: AssignTarget, expr: Expr, op: AssignOp },
    Print { args: Vec<Expr> },
    ExprStmt { expr: Expr },
    Try {
        body: Vec<Stmt>,
        handlers: Vec<ExceptHandler>,
        finally_body: Option<Vec<Stmt>>,
    },
    Raise { expr: Expr },
    If {
        branches: Vec<(Expr, Vec<Stmt>)>,
        else_branch: Option<Vec<Stmt>>,
    },
    While { cond: Expr, body: Vec<Stmt> },
    For { target: ForTarget, iter: Expr, body: Vec<Stmt> },
    With { target: String, expr: Expr, body: Vec<Stmt> },
    Return { expr: Option<Expr> },
}

#[derive(Debug, Clone)]
struct ExceptHandler {
    name: Option<String>,
    bind: Option<String>,
    body: Vec<Stmt>,
}

#[derive(Debug, Clone)]
enum AssignTarget {
    Name(String),
    Subscript { object: Expr, index: Expr },
    Attr { object: Expr, attr: String },
    Tuple { items: Vec<AssignTarget> },
    Starred(String),
}

#[derive(Debug, Clone, Copy)]
enum AssignOp {
    Assign,
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Debug, Clone)]
enum ForTarget {
    Name(String),
    Tuple(Vec<String>),
}

#[derive(Debug, Clone)]
enum Expr {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
    Var(String),
    Lambda { params: Vec<String>, body: Box<Expr> },
    Unary(UnOp, Box<Expr>),
    Binary(Box<Expr>, BinOp, Box<Expr>),
    Compare(Box<Expr>, CmpOp, Box<Expr>),
    Call { callee: Box<Expr>, args: Vec<Expr> },
    List(Vec<Expr>),
    Tuple(Vec<Expr>),
    Dict(Vec<(Expr, Expr)>),
    ListComp {
        target: ForTarget,
        iter: Box<Expr>,
        expr: Box<Expr>,
        cond: Option<Box<Expr>>,
    },
    DictComp {
        target: ForTarget,
        iter: Box<Expr>,
        key: Box<Expr>,
        value: Box<Expr>,
        cond: Option<Box<Expr>>,
    },
    Set(Vec<Expr>),
    Index(Box<Expr>, Box<Expr>),
    Slice {
        value: Box<Expr>,
        start: Option<Box<Expr>>,
        end: Option<Box<Expr>>,
    },
    MethodCall {
        object: Box<Expr>,
        method: String,
        arg: Option<Box<Expr>>,
    },
    Attr {
        object: Box<Expr>,
        attr: String,
    },
}

#[derive(Debug, Clone, Copy)]
enum UnOp {
    Neg,
}

#[derive(Debug, Clone, Copy)]
enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Debug, Clone, Copy)]
enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug)]
struct ParseError {
    message: String,
    line: usize,
    column: usize,
    line_src: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let caret_pos = self.column.saturating_sub(1);
        let mut caret_line = String::new();
        for _ in 0..caret_pos {
            caret_line.push(' ');
        }
        caret_line.push('^');
        write!(
            f,
            "Parse error at line {}, col {}: {}\n{}\n{}",
            self.line, self.column, self.message, self.line_src, caret_line
        )
    }
}

impl std::error::Error for ParseError {}

fn parse_error(line: usize, column: usize, message: &str, line_src: &str) -> ParseError {
    ParseError {
        message: message.to_string(),
        line,
        column,
        line_src: line_src.to_string(),
    }
}

#[derive(Debug)]
struct LineInfo {
    line_no: usize,
    indent: usize,
    content: String,
    raw: String,
}

fn collect_lines(source: &str) -> Vec<LineInfo> {
    let mut lines = Vec::new();
    for (idx, raw_line) in source.lines().enumerate() {
        let line_no = idx + 1;
        let trimmed_end = raw_line.trim_end();
        if trimmed_end.trim().is_empty() {
            continue;
        }
        let without_indent = trimmed_end.trim_start_matches(|c: char| c == ' ' || c == '\t');
        let indent = trimmed_end.len() - without_indent.len();
        let content = without_indent.trim().to_string();
        if content.is_empty() {
            continue;
        }
        lines.push(LineInfo {
            line_no,
            indent,
            content,
            raw: raw_line.to_string(),
        });
    }
    lines
}

fn parse_program(source: &str) -> Result<Program, ParseError> {
    let lines = collect_lines(source);
    let (stmts, next) = parse_block(&lines, 0, 0)?;
    if next != lines.len() {
        let line = &lines[next];
        return Err(parse_error(
            line.line_no,
            line.indent + 1,
            "Failed to parse entire file",
            &line.raw,
        ));
    }
    Ok(Program { stmts })
}

fn parse_block(lines: &[LineInfo], start: usize, indent: usize) -> Result<(Vec<Stmt>, usize), ParseError> {
    let mut stmts = Vec::new();
    let mut idx = start;
    while idx < lines.len() {
        let line = &lines[idx];
        if line.indent < indent {
            break;
        }
        if line.indent > indent {
            return Err(parse_error(
                line.line_no,
                line.indent + 1,
                "Unexpected indent",
                &line.raw,
            ));
        }
        let (stmt, next_idx) = parse_statement(lines, idx, indent)?;
        stmts.push(stmt);
        idx = next_idx;
    }
    Ok((stmts, idx))
}

fn parse_statement(lines: &[LineInfo], idx: usize, indent: usize) -> Result<(Stmt, usize), ParseError> {
    let line = &lines[idx];
    let content = line.content.as_str();
    let col_offset = line.indent + 1;
    if content.starts_with("def ") {
        parse_def(lines, idx, indent)
    } else if content.starts_with("return") {
        parse_return(lines, idx)
    } else if content.starts_with("class ") {
        parse_class(lines, idx, indent)
    } else if content.starts_with("try") {
        parse_try(lines, idx, indent)
    } else if content.starts_with("raise") {
        parse_raise(lines, idx)
    } else if content.starts_with("if ") || content.starts_with("if(") {
        parse_if(lines, idx, indent)
    } else if content.starts_with("while ") || content.starts_with("while(") {
        parse_while(lines, idx, indent)
    } else if content.starts_with("for ") {
        parse_for(lines, idx, indent)
    } else if content.starts_with("with ") {
        parse_with(lines, idx, indent)
    } else if content.starts_with("print") {
        let stmt = parse_print(content, line.line_no, col_offset, &line.raw)?;
        Ok((stmt, idx + 1))
    } else if find_assign_op(content).is_some() {
        let stmt = parse_assign(content, line.line_no, col_offset, &line.raw)?;
        Ok((stmt, idx + 1))
    } else {
        let (expr, next) = parse_expression(content, 0, line.line_no, col_offset, &line.raw)?;
        let next = skip_ws(content.as_bytes(), next);
        if next != content.len() {
            return Err(parse_error(
                line.line_no,
                col_offset + next,
                "Unexpected trailing characters",
                &line.raw,
            ));
        }
        Ok((Stmt::ExprStmt { expr }, idx + 1))
    }
}

fn parse_if(lines: &[LineInfo], idx: usize, indent: usize) -> Result<(Stmt, usize), ParseError> {
    let line = &lines[idx];
    let content = line.content.as_str();
    if !content.starts_with("if") {
        return Err(parse_error(line.line_no, line.indent + 1, "Expected 'if'", &line.raw));
    }
    let colon = content.find(':').ok_or_else(|| {
        parse_error(
            line.line_no,
            line.indent + content.len(),
            "Expected ':' after if condition",
            &line.raw,
        )
    })?;
    let cond_src = content[2..colon].trim();
    let cond_offset = line.raw.find(cond_src).unwrap_or(line.indent) + 1;
    let (cond, _) = parse_expression(cond_src, 0, line.line_no, cond_offset, &line.raw)?;
    if !content[colon + 1..].trim().is_empty() {
        return Err(parse_error(
            line.line_no,
            line.indent + colon + 2,
            "Unexpected characters after ':'",
            &line.raw,
        ));
    }
    if idx + 1 >= lines.len() || lines[idx + 1].indent <= indent {
        return Err(parse_error(
            line.line_no,
            line.indent + colon + 1,
            "Missing indented block after if",
            &line.raw,
        ));
    }
    let body_indent = lines[idx + 1].indent;
    let (body, mut next_idx) = parse_block(lines, idx + 1, body_indent)?;
    let mut branches = vec![(cond, body)];
    let mut else_branch = None;
    while next_idx < lines.len() && lines[next_idx].indent == indent {
        let nxt = &lines[next_idx];
        if nxt.content.starts_with("elif") {
            let colon = nxt.content.find(':').ok_or_else(|| {
                parse_error(
                    nxt.line_no,
                    nxt.indent + nxt.content.len(),
                    "Expected ':' after elif condition",
                    &nxt.raw,
                )
            })?;
            let cond_src = nxt.content[4..colon].trim();
            let cond_offset = nxt.raw.find(cond_src).unwrap_or(nxt.indent) + 1;
            let (elif_cond, _) = parse_expression(cond_src, 0, nxt.line_no, cond_offset, &nxt.raw)?;
            if !nxt.content[colon + 1..].trim().is_empty() {
                return Err(parse_error(
                    nxt.line_no,
                    nxt.indent + colon + 2,
                    "Unexpected characters after ':'",
                    &nxt.raw,
                ));
            }
            if next_idx + 1 >= lines.len() || lines[next_idx + 1].indent <= indent {
                return Err(parse_error(
                    nxt.line_no,
                    nxt.indent + colon + 1,
                    "Missing indented block after elif",
                    &nxt.raw,
                ));
            }
            let body_indent = lines[next_idx + 1].indent;
            let (elif_body, n2) = parse_block(lines, next_idx + 1, body_indent)?;
            branches.push((elif_cond, elif_body));
            next_idx = n2;
            continue;
        } else if nxt.content.starts_with("else") {
            let colon = nxt.content.find(':').ok_or_else(|| {
                parse_error(
                    nxt.line_no,
                    nxt.indent + nxt.content.len(),
                    "Expected ':' after else",
                    &nxt.raw,
                )
            })?;
            if !nxt.content[colon + 1..].trim().is_empty() {
                return Err(parse_error(
                    nxt.line_no,
                    nxt.indent + colon + 2,
                    "Unexpected characters after ':'",
                    &nxt.raw,
                ));
            }
            if next_idx + 1 >= lines.len() || lines[next_idx + 1].indent <= indent {
                return Err(parse_error(
                    nxt.line_no,
                    nxt.indent + colon + 1,
                    "Missing indented block after else",
                    &nxt.raw,
                ));
            }
            let body_indent = lines[next_idx + 1].indent;
            let (body, n2) = parse_block(lines, next_idx + 1, body_indent)?;
            else_branch = Some(body);
            next_idx = n2;
            break;
        } else {
            break;
        }
    }
    Ok((Stmt::If { branches, else_branch }, next_idx))
}

fn parse_while(lines: &[LineInfo], idx: usize, indent: usize) -> Result<(Stmt, usize), ParseError> {
    let line = &lines[idx];
    let content = line.content.as_str();
    let colon = content.find(':').ok_or_else(|| {
        parse_error(
            line.line_no,
            line.indent + content.len(),
            "Expected ':' after while condition",
            &line.raw,
        )
    })?;
    let cond_src = content[5..colon].trim();
    let cond_offset = line.raw.find(cond_src).unwrap_or(line.indent) + 1;
    let (cond, _) = parse_expression(cond_src, 0, line.line_no, cond_offset, &line.raw)?;
    if !content[colon + 1..].trim().is_empty() {
        return Err(parse_error(
            line.line_no,
            line.indent + colon + 2,
            "Unexpected characters after ':'",
            &line.raw,
        ));
    }
    if idx + 1 >= lines.len() || lines[idx + 1].indent <= indent {
        return Err(parse_error(
            line.line_no,
            line.indent + colon + 1,
            "Missing indented block after while",
            &line.raw,
        ));
    }
    let body_indent = lines[idx + 1].indent;
    let (body, next_idx) = parse_block(lines, idx + 1, body_indent)?;
    Ok((Stmt::While { cond, body }, next_idx))
}

fn parse_for(lines: &[LineInfo], idx: usize, indent: usize) -> Result<(Stmt, usize), ParseError> {
    let line = &lines[idx];
    let content = line.content.as_str();
    let colon = content.find(':').ok_or_else(|| {
        parse_error(
            line.line_no,
            line.indent + content.len(),
            "Expected ':' after for clause",
            &line.raw,
        )
    })?;
    let head = content[3..colon].trim();
    let parts: Vec<&str> = head.splitn(2, " in ").collect();
    if parts.len() != 2 {
        return Err(parse_error(
            line.line_no,
            line.indent + 1,
            "Expected 'for <target> in <iter>:'",
            &line.raw,
        ));
    }
    let target_src = parts[0].trim();
    let iter_src = parts[1].trim();
    let target = parse_for_target(target_src, line.line_no, line.indent + 1, &line.raw)?;
    let iter_offset = line.raw.find(iter_src).unwrap_or(line.indent) + 1;
    let (iter_expr, _) = parse_expression(iter_src, 0, line.line_no, iter_offset, &line.raw)?;
    if !content[colon + 1..].trim().is_empty() {
        return Err(parse_error(
            line.line_no,
            line.indent + colon + 2,
            "Unexpected characters after ':'",
            &line.raw,
        ));
    }
    if idx + 1 >= lines.len() || lines[idx + 1].indent <= indent {
        return Err(parse_error(
            line.line_no,
            line.indent + colon + 1,
            "Missing indented block after for",
            &line.raw,
        ));
    }
    let body_indent = lines[idx + 1].indent;
    let (body, next_idx) = parse_block(lines, idx + 1, body_indent)?;
    Ok((Stmt::For { target, iter: iter_expr, body }, next_idx))
}

fn parse_def(lines: &[LineInfo], idx: usize, indent: usize) -> Result<(Stmt, usize), ParseError> {
    let line = &lines[idx];
    let content = line.content.as_str();
    let colon = content.find(':').ok_or_else(|| {
        parse_error(
            line.line_no,
            line.indent + content.len(),
            "Expected ':' after function signature",
            &line.raw,
        )
    })?;
    let head = content[3..colon].trim();
    let open = head.find('(').ok_or_else(|| parse_error(line.line_no, line.indent + 4, "Expected '('", &line.raw))?;
    let close = head.rfind(')').ok_or_else(|| parse_error(line.line_no, line.indent + head.len(), "Missing ')'", &line.raw))?;
    let name = head[..open].trim();
    if !is_valid_ident(name) {
        return Err(parse_error(line.line_no, line.indent + 1, "Invalid function name", &line.raw));
    }
    let params_src = &head[(open + 1)..close];
    let mut params = Vec::new();
    if !params_src.trim().is_empty() {
        for p in params_src.split(',') {
            let p = p.trim();
            if !is_valid_ident(p) {
                return Err(parse_error(line.line_no, line.indent + 1, "Invalid parameter", &line.raw));
            }
            params.push(p.to_string());
        }
    }
    if !content[colon + 1..].trim().is_empty() {
        return Err(parse_error(
            line.line_no,
            line.indent + colon + 2,
            "Unexpected characters after ':'",
            &line.raw,
        ));
    }
    if idx + 1 >= lines.len() || lines[idx + 1].indent <= indent {
        return Err(parse_error(
            line.line_no,
            line.indent + colon + 1,
            "Missing indented block after function",
            &line.raw,
        ));
    }
    let body_indent = lines[idx + 1].indent;
    let (body, next_idx) = parse_block(lines, idx + 1, body_indent)?;
    Ok((Stmt::Function { name: name.to_string(), params, body }, next_idx))
}

fn parse_class(lines: &[LineInfo], idx: usize, indent: usize) -> Result<(Stmt, usize), ParseError> {
    let line = &lines[idx];
    let content = line.content.as_str();
    let colon = content.find(':').ok_or_else(|| {
        parse_error(
            line.line_no,
            line.indent + content.len(),
            "Expected ':' after class name",
            &line.raw,
        )
    })?;
    let name = content[5..colon].trim();
    if !is_valid_ident(name) {
        return Err(parse_error(line.line_no, line.indent + 1, "Invalid class name", &line.raw));
    }
    if !content[colon + 1..].trim().is_empty() {
        return Err(parse_error(
            line.line_no,
            line.indent + colon + 2,
            "Unexpected characters after ':'",
            &line.raw,
        ));
    }
    if idx + 1 >= lines.len() || lines[idx + 1].indent <= indent {
        return Err(parse_error(
            line.line_no,
            line.indent + colon + 1,
            "Missing indented block after class",
            &line.raw,
        ));
    }
    let body_indent = lines[idx + 1].indent;
    let (body, next_idx) = parse_block(lines, idx + 1, body_indent)?;
    // filter methods
    let mut methods = Vec::new();
    for stmt in body {
        match stmt {
            Stmt::Function { .. } => methods.push(stmt),
            _ => {
                return Err(parse_error(
                    line.line_no,
                    line.indent + 1,
                    "Only method definitions are allowed inside class",
                    &line.raw,
                ))
            }
        }
    }
    Ok((Stmt::Class { name: name.to_string(), methods }, next_idx))
}

fn parse_return(lines: &[LineInfo], idx: usize) -> Result<(Stmt, usize), ParseError> {
    let line = &lines[idx];
    let content = line.content.as_str();
    let rest = content.strip_prefix("return").unwrap().trim();
    if rest.is_empty() {
        return Ok((Stmt::Return { expr: None }, idx + 1));
    }
    let col_offset = line.indent + line.content.find(rest).unwrap_or(0) + 1;
    let (expr, next) = parse_expression(rest, 0, line.line_no, col_offset, &line.raw)?;
    let next = skip_ws(rest.as_bytes(), next);
    if next != rest.len() {
        return Err(parse_error(
            line.line_no,
            col_offset + next,
            "Unexpected trailing characters after return expression",
            &line.raw,
        ));
    }
    Ok((Stmt::Return { expr: Some(expr) }, idx + 1))
}

fn parse_raise(lines: &[LineInfo], idx: usize) -> Result<(Stmt, usize), ParseError> {
    let line = &lines[idx];
    let content = line.content.as_str();
    let rest = content.strip_prefix("raise").unwrap().trim();
    if rest.is_empty() {
        return Err(parse_error(
            line.line_no,
            line.indent + 1 + "raise".len(),
            "Missing expression after raise",
            &line.raw,
        ));
    }
    let col_offset = line.indent + line.content.find(rest).unwrap_or(0) + 1;
    let (expr, next) = parse_expression(rest, 0, line.line_no, col_offset, &line.raw)?;
    let next = skip_ws(rest.as_bytes(), next);
    if next != rest.len() {
        return Err(parse_error(
            line.line_no,
            col_offset + next,
            "Unexpected trailing characters after raise expression",
            &line.raw,
        ));
    }
    Ok((Stmt::Raise { expr }, idx + 1))
}

fn parse_try(lines: &[LineInfo], idx: usize, indent: usize) -> Result<(Stmt, usize), ParseError> {
    let line = &lines[idx];
    let content = line.content.as_str();
    let colon = content.find(':').ok_or_else(|| {
        parse_error(
            line.line_no,
            line.indent + content.len(),
            "Expected ':' after try",
            &line.raw,
        )
    })?;
    if !content[colon + 1..].trim().is_empty() {
        return Err(parse_error(
            line.line_no,
            line.indent + colon + 2,
            "Unexpected characters after ':'",
            &line.raw,
        ));
    }
    if idx + 1 >= lines.len() || lines[idx + 1].indent <= indent {
        return Err(parse_error(
            line.line_no,
            line.indent + colon + 1,
            "Missing indented block after try",
            &line.raw,
        ));
    }
    let body_indent = lines[idx + 1].indent;
    let (body, mut next_idx) = parse_block(lines, idx + 1, body_indent)?;
    let mut handlers = Vec::new();
    let mut finally_body = None;
    while next_idx < lines.len() && lines[next_idx].indent == indent {
        let nxt = &lines[next_idx];
        if nxt.content.starts_with("except") {
            let colon_pos = nxt.content.find(':').ok_or_else(|| {
                parse_error(
                    nxt.line_no,
                    nxt.indent + nxt.content.len(),
                    "Expected ':' after except",
                    &nxt.raw,
                )
            })?;
            let head = nxt.content["except".len()..colon_pos].trim();
            let mut name = None;
            let mut bind = None;
            if !head.is_empty() {
                let parts: Vec<&str> = head.splitn(2, " as ").collect();
                let type_name = parts[0].trim();
                if !type_name.is_empty() {
                    if !is_valid_ident(type_name) {
                        return Err(parse_error(
                            nxt.line_no,
                            nxt.indent + 1,
                            "Invalid exception name",
                            &nxt.raw,
                        ));
                    }
                    name = Some(type_name.to_string());
                }
                if parts.len() == 2 {
                    let bind_name = parts[1].trim();
                    if !bind_name.is_empty() {
                        if !is_valid_ident(bind_name) {
                            return Err(parse_error(
                                nxt.line_no,
                                nxt.indent + 1,
                                "Invalid exception binding name",
                                &nxt.raw,
                            ));
                        }
                        bind = Some(bind_name.to_string());
                    }
                }
            }
            if !nxt.content[colon_pos + 1..].trim().is_empty() {
                return Err(parse_error(
                    nxt.line_no,
                    nxt.indent + colon_pos + 2,
                    "Unexpected characters after ':'",
                    &nxt.raw,
                ));
            }
            if next_idx + 1 >= lines.len() || lines[next_idx + 1].indent <= indent {
                return Err(parse_error(
                    nxt.line_no,
                    nxt.indent + colon_pos + 1,
                    "Missing indented block after except",
                    &nxt.raw,
                ));
            }
            let body_indent = lines[next_idx + 1].indent;
            let (handler_body, n2) = parse_block(lines, next_idx + 1, body_indent)?;
            handlers.push(ExceptHandler { name, bind, body: handler_body });
            next_idx = n2;
            continue;
        }
        if nxt.content.starts_with("finally") {
            if finally_body.is_some() {
                return Err(parse_error(
                    nxt.line_no,
                    nxt.indent + 1,
                    "Multiple finally blocks not allowed",
                    &nxt.raw,
                ));
            }
            let colon_pos = nxt.content.find(':').ok_or_else(|| {
                parse_error(
                    nxt.line_no,
                    nxt.indent + nxt.content.len(),
                    "Expected ':' after finally",
                    &nxt.raw,
                )
            })?;
            if !nxt.content[colon_pos + 1..].trim().is_empty() {
                return Err(parse_error(
                    nxt.line_no,
                    nxt.indent + colon_pos + 2,
                    "Unexpected characters after ':'",
                    &nxt.raw,
                ));
            }
            if next_idx + 1 >= lines.len() || lines[next_idx + 1].indent <= indent {
                return Err(parse_error(
                    nxt.line_no,
                    nxt.indent + colon_pos + 1,
                    "Missing indented block after finally",
                    &nxt.raw,
                ));
            }
            let body_indent = lines[next_idx + 1].indent;
            let (f_body, n2) = parse_block(lines, next_idx + 1, body_indent)?;
            finally_body = Some(f_body);
            next_idx = n2;
            continue;
        }
        break;
    }
    if handlers.is_empty() && finally_body.is_none() {
        return Err(parse_error(
            line.line_no,
            line.indent + 1,
            "try must have except or finally",
            &line.raw,
        ));
    }
    Ok((Stmt::Try { body, handlers, finally_body }, next_idx))
}

fn parse_with(lines: &[LineInfo], idx: usize, indent: usize) -> Result<(Stmt, usize), ParseError> {
    let line = &lines[idx];
    let content = line.content.as_str();
    let colon = content.find(':').ok_or_else(|| {
        parse_error(
            line.line_no,
            line.indent + content.len(),
            "Expected ':' after with clause",
            &line.raw,
        )
    })?;
    let head = content[4..colon].trim();
    let parts: Vec<&str> = head.splitn(2, " as ").collect();
    if parts.len() != 2 {
        return Err(parse_error(
            line.line_no,
            line.indent + 1,
            "Expected 'with <expr> as <name>:'",
            &line.raw,
        ));
    }
    let expr_src = parts[0].trim();
    let target_src = parts[1].trim();
    if !is_valid_ident(target_src) {
        return Err(parse_error(
            line.line_no,
            line.indent + 1,
            "Invalid target in with statement",
            &line.raw,
        ));
    }
    let expr_offset = line.raw.find(expr_src).unwrap_or(line.indent) + 1;
    let (expr, _) = parse_expression(expr_src, 0, line.line_no, expr_offset, &line.raw)?;
    if !content[colon + 1..].trim().is_empty() {
        return Err(parse_error(
            line.line_no,
            line.indent + colon + 2,
            "Unexpected characters after ':'",
            &line.raw,
        ));
    }
    if idx + 1 >= lines.len() || lines[idx + 1].indent <= indent {
        return Err(parse_error(
            line.line_no,
            line.indent + colon + 1,
            "Missing indented block after with",
            &line.raw,
        ));
    }
    let body_indent = lines[idx + 1].indent;
    let (body, next_idx) = parse_block(lines, idx + 1, body_indent)?;
    Ok((Stmt::With { target: target_src.to_string(), expr, body }, next_idx))
}

fn parse_for_target(segment: &str, line_no: usize, column_offset: usize, line_src: &str) -> Result<ForTarget, ParseError> {
    let mut seg = segment.trim();
    if seg.starts_with('(') && seg.ends_with(')') {
        seg = &seg[1..seg.len().saturating_sub(1)];
    }
    if seg.contains(',') {
        let mut names = Vec::new();
        for part in seg.split(',') {
            let name = part.trim();
            if name.is_empty() {
                return Err(parse_error(
                    line_no,
                    column_offset,
                    "Empty name in tuple target",
                    line_src,
                ));
            }
            if !is_valid_ident(name) {
                return Err(parse_error(
                    line_no,
                    column_offset,
                    "Invalid identifier in tuple target",
                    line_src,
                ));
            }
            names.push(name.to_string());
        }
        Ok(ForTarget::Tuple(names))
    } else {
        let name = seg.trim();
        if !is_valid_ident(name) {
            return Err(parse_error(
                line_no,
                column_offset,
                "Invalid for-loop target",
                line_src,
            ));
        }
        Ok(ForTarget::Name(name.to_string()))
    }
}

fn find_assign_op(line: &str) -> Option<(usize, AssignOp)> {
    let bytes = line.as_bytes();
    let mut in_string = false;
    let mut i = 0;
    while i < bytes.len() {
        let ch = bytes[i];
        if in_string {
            if ch == b'\\' {
                i += 1;
            } else if ch == b'"' {
                in_string = false;
            }
            i += 1;
            continue;
        }
        if ch == b'"' {
            in_string = true;
            i += 1;
            continue;
        }
        if ch == b'=' {
            let prev = if i > 0 { bytes[i - 1] } else { 0 };
            let next = if i + 1 < bytes.len() { bytes[i + 1] } else { 0 };
            // skip ==, !=, <=, >=
            if prev == b'=' || prev == b'!' || prev == b'<' || prev == b'>' {
                i += 1;
                continue;
            }
            if next == b'=' {
                i += 2;
                continue;
            }
            return if prev == b'+' {
                Some((i - 1, AssignOp::Add))
            } else if prev == b'-' {
                Some((i - 1, AssignOp::Sub))
            } else if prev == b'*' {
                Some((i - 1, AssignOp::Mul))
            } else if prev == b'/' {
                Some((i - 1, AssignOp::Div))
            } else {
                Some((i, AssignOp::Assign))
            };
        }
        i += 1;
    }
    None
}

fn parse_print(line: &str, line_no: usize, column_offset: usize, line_src: &str) -> Result<Stmt, ParseError> {
    let prefix = "print";
    if !line.starts_with(prefix) {
        return Err(parse_error(
            line_no,
            column_offset,
            "Only 'print' statements are supported here",
            line_src,
        ));
    }
    let mut rest = &line[prefix.len()..];
    rest = rest.trim_start();
    if !rest.starts_with('(') {
        return Err(parse_error(
            line_no,
            column_offset + prefix.len(),
            "Expected '(' after 'print'",
            line_src,
        ));
    }
    let open = line.find('(').unwrap();
    let close = line
        .rfind(')')
        .ok_or_else(|| parse_error(line_no, column_offset + line.len(), "Missing ')'", line_src))?;
    let inside = &line[(open + 1)..close];
    let args = parse_args(inside, line_no, column_offset + open + 1, line_src)?;
    let trailing = line[(close + 1)..].trim();
    if !trailing.is_empty() {
        return Err(parse_error(
            line_no,
            column_offset + close + 1,
            "Unexpected characters after ')'",
            line_src,
        ));
    }
    Ok(Stmt::Print { args })
}

fn parse_assign(line: &str, line_no: usize, column_offset: usize, line_src: &str) -> Result<Stmt, ParseError> {
    let (op_pos, op) = find_assign_op(line).ok_or_else(|| parse_error(line_no, column_offset, "Expected '='", line_src))?;
    let op_len = if matches!(op, AssignOp::Assign) { 1 } else { 2 };
    let (left, right) = line.split_at(op_pos);
    let lhs_str = left.trim();
    if lhs_str.is_empty() {
        return Err(parse_error(line_no, column_offset, "Missing assignment target", line_src));
    }
    let target = parse_assign_target(lhs_str, line_no, column_offset, line_src)?;
    let expr_str = right[op_len..].trim();
    if expr_str.is_empty() {
        return Err(parse_error(
            line_no,
            column_offset + op_pos + op_len,
            "Missing expression on right-hand side",
            line_src,
        ));
    }
    let (expr, next) =
        parse_expression(expr_str, 0, line_no, line_src.find(expr_str).unwrap_or(column_offset) + 1, line_src)?;
    let next = skip_ws(expr_str.as_bytes(), next);
    if next != expr_str.len() {
        return Err(parse_error(
            line_no,
            line_src.find(expr_str).unwrap_or(column_offset + op_pos) + next + 1,
            "Unexpected trailing characters",
            line_src,
        ));
    }
    Ok(Stmt::Assign { target, expr, op })
}

fn parse_assign_target(segment: &str, line_no: usize, column_offset: usize, line_src: &str) -> Result<AssignTarget, ParseError> {
    // split on top-level commas
    let mut depth = 0;
    let bytes = segment.as_bytes();
    let mut parts = Vec::new();
    let mut start = 0;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\\' {
                        i += 2;
                        continue;
                    }
                    if bytes[i] == b'"' {
                        break;
                    }
                    i += 1;
                }
            }
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => {
                if depth > 0 {
                    depth -= 1;
                }
            }
            b',' if depth == 0 => {
                parts.push(segment[start..i].trim().to_string());
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    parts.push(segment[start..].trim().to_string());
    if parts.len() > 1 {
        let mut items = Vec::new();
        let mut star_seen = false;
        for part in &parts {
            if part.is_empty() {
                return Err(parse_error(
                    line_no,
                    column_offset,
                    "Empty element in unpacking assignment",
                    line_src,
                ));
            }
            if part.starts_with('*') {
                if star_seen {
                    return Err(parse_error(
                        line_no,
                        column_offset,
                        "Only one starred target allowed",
                        line_src,
                    ));
                }
                star_seen = true;
                let name = part.trim_start_matches('*').trim();
                if !is_valid_ident(name) {
                    return Err(parse_error(
                        line_no,
                        column_offset,
                        "Invalid starred target",
                        line_src,
                    ));
                }
                items.push(AssignTarget::Starred(name.to_string()));
            } else {
                let inner = parse_assign_target(part, line_no, column_offset, line_src)?;
                items.push(inner);
            }
        }
        return Ok(AssignTarget::Tuple { items });
    }
    let single = parts[0].trim();
    let lhs_offset = line_src
        .find(single)
        .map(|idx| column_offset + idx)
        .unwrap_or(column_offset);
    if single.starts_with('*') {
        return Err(parse_error(
            line_no,
            lhs_offset,
            "Starred target not allowed here",
            line_src,
        ));
    }
    let (lhs_expr, lhs_next) = parse_expression(single, 0, line_no, lhs_offset, line_src)?;
    let lhs_next = skip_ws(single.as_bytes(), lhs_next);
    if lhs_next != single.len() {
        return Err(parse_error(
            line_no,
            lhs_offset + lhs_next,
            "Unexpected trailing characters in assignment target",
            line_src,
        ));
    }
    match lhs_expr {
        Expr::Var(name) => Ok(AssignTarget::Name(name)),
        Expr::Index(obj, idx) => match *obj {
            Expr::Var(name) => Ok(AssignTarget::Subscript { object: Expr::Var(name), index: *idx }),
            _ => Err(parse_error(
                line_no,
                lhs_offset,
                "Can only assign to a variable subscript",
                line_src,
            )),
        },
        Expr::Attr { object, attr } => match *object {
            Expr::Var(name) => Ok(AssignTarget::Attr { object: Expr::Var(name), attr }),
            Expr::Attr { .. } => Ok(AssignTarget::Attr { object: *object, attr }),
            _ => Err(parse_error(
                line_no,
                lhs_offset,
                "Can only assign to an attribute of a variable or attribute",
                line_src,
            )),
        },
        _ => Err(parse_error(
            line_no,
            lhs_offset,
            "Unsupported assignment target",
            line_src,
        )),
    }
}

fn parse_args(segment: &str, line_no: usize, column_offset: usize, line_src: &str) -> Result<Vec<Expr>, ParseError> {
    let mut args = Vec::new();
    let bytes = segment.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        i = skip_ws(bytes, i);
        if i >= bytes.len() {
            break;
        }
        let (expr, next) = parse_expression(segment, i, line_no, column_offset, line_src)?;
        args.push(expr);
        i = skip_ws(bytes, next);
        if i < bytes.len() {
            if bytes[i] == b',' {
                i += 1;
            } else {
                return Err(parse_error(
                    line_no,
                    column_offset + i,
                    "Expected ',' between arguments",
                    line_src,
                ));
            }
        }
    }
    Ok(args)
}

fn parse_expression(
    segment: &str,
    start: usize,
    line_no: usize,
    column_offset: usize,
    line_src: &str,
) -> Result<(Expr, usize), ParseError> {
    let bytes = segment.as_bytes();
    let i = skip_ws(bytes, start);
    if i < bytes.len() && segment[i..].trim_start().starts_with("lambda") {
        return parse_lambda(segment, i, line_no, column_offset, line_src);
    }
    parse_comparison(segment, start, line_no, column_offset, line_src)
}

fn parse_lambda(
    segment: &str,
    start: usize,
    line_no: usize,
    column_offset: usize,
    line_src: &str,
) -> Result<(Expr, usize), ParseError> {
    let mut i = start;
    let bytes = segment.as_bytes();
    i = skip_ws(bytes, i);
    if !segment[i..].starts_with("lambda") {
        return Err(parse_error(line_no, column_offset + i + 1, "Expected lambda", line_src));
    }
    i += "lambda".len();
    i = skip_ws(bytes, i);
    let params_start = i;
    while i < bytes.len() && bytes[i] != b':' {
        i += 1;
    }
    if i >= bytes.len() {
        return Err(parse_error(
            line_no,
            column_offset + params_start + 1,
            "Missing ':' in lambda",
            line_src,
        ));
    }
    let params_src = &segment[params_start..i];
    let mut params = Vec::new();
    if !params_src.trim().is_empty() {
        for p in params_src.split(',') {
            let p = p.trim();
            if !is_valid_ident(p) {
                return Err(parse_error(line_no, column_offset + params_start + 1, "Invalid lambda param", line_src));
            }
            params.push(p.to_string());
        }
    }
    i += 1; // skip ':'
    let body_start = skip_ws(bytes, i);
    let (body_expr, next) = parse_expression(segment, body_start, line_no, column_offset, line_src)?;
    Ok((Expr::Lambda { params, body: Box::new(body_expr) }, next))
}

fn parse_comparison(
    segment: &str,
    start: usize,
    line_no: usize,
    column_offset: usize,
    line_src: &str,
) -> Result<(Expr, usize), ParseError> {
    let (left, mut i) = parse_add_sub(segment, start, line_no, column_offset, line_src)?;
    let bytes = segment.as_bytes();
    i = skip_ws(bytes, i);
    if i >= bytes.len() {
        return Ok((left, i));
    }
    let op = match bytes.get(i..i + 2) {
        Some(b"==") => {
            i += 2;
            CmpOp::Eq
        }
        Some(b"!=") => {
            i += 2;
            CmpOp::Ne
        }
        Some(b"<=") => {
            i += 2;
            CmpOp::Le
        }
        Some(b">=") => {
            i += 2;
            CmpOp::Ge
        }
        _ => match bytes[i] {
            b'<' => {
                i += 1;
                CmpOp::Lt
            }
            b'>' => {
                i += 1;
                CmpOp::Gt
            }
            _ => return Ok((left, i)),
        },
    };
    let (right, next) = parse_add_sub(segment, i, line_no, column_offset, line_src)?;
    Ok((Expr::Compare(Box::new(left), op, Box::new(right)), next))
}

fn parse_add_sub(
    segment: &str,
    start: usize,
    line_no: usize,
    column_offset: usize,
    line_src: &str,
) -> Result<(Expr, usize), ParseError> {
    let (mut left, mut i) = parse_mul_div(segment, start, line_no, column_offset, line_src)?;
    let bytes = segment.as_bytes();
    i = skip_ws(bytes, i);
    while i < bytes.len() {
        let op = match bytes[i] {
            b'+' => BinOp::Add,
            b'-' => BinOp::Sub,
            _ => break,
        };
        i += 1;
        i = skip_ws(bytes, i);
        let (right, next) = parse_mul_div(segment, i, line_no, column_offset, line_src)?;
        left = Expr::Binary(Box::new(left), op, Box::new(right));
        i = skip_ws(bytes, next);
    }
    Ok((left, i))
}

fn parse_mul_div(
    segment: &str,
    start: usize,
    line_no: usize,
    column_offset: usize,
    line_src: &str,
) -> Result<(Expr, usize), ParseError> {
    let (mut left, mut i) = parse_unary(segment, start, line_no, column_offset, line_src)?;
    let bytes = segment.as_bytes();
    i = skip_ws(bytes, i);
    while i < bytes.len() {
        let op = match bytes[i] {
            b'*' => BinOp::Mul,
            b'/' => BinOp::Div,
            _ => break,
        };
        i += 1;
        i = skip_ws(bytes, i);
        let (right, next) = parse_unary(segment, i, line_no, column_offset, line_src)?;
        left = Expr::Binary(Box::new(left), op, Box::new(right));
        i = skip_ws(bytes, next);
    }
    Ok((left, i))
}

fn parse_unary(
    segment: &str,
    start: usize,
    line_no: usize,
    column_offset: usize,
    line_src: &str,
) -> Result<(Expr, usize), ParseError> {
    let bytes = segment.as_bytes();
    let i = skip_ws(bytes, start);
    if i < bytes.len() && bytes[i] == b'-' {
        let (expr, next) = parse_primary(segment, i + 1, line_no, column_offset, line_src)?;
        return Ok((Expr::Unary(UnOp::Neg, Box::new(expr)), next));
    }
    parse_primary(segment, i, line_no, column_offset, line_src)
}

fn parse_primary(
    segment: &str,
    start: usize,
    line_no: usize,
    column_offset: usize,
    line_src: &str,
) -> Result<(Expr, usize), ParseError> {
    let bytes = segment.as_bytes();
    let i = skip_ws(bytes, start);
    if i >= bytes.len() {
        return Err(parse_error(
            line_no,
            column_offset + i,
            "Unexpected end of expression",
            line_src,
        ));
    }
    let (mut expr, mut idx) = match bytes[i] {
        b'"' => parse_string(segment, i, line_no, column_offset, line_src),
        b'0'..=b'9' => parse_number(segment, i, line_no, column_offset, line_src),
        b'(' => parse_tuple_or_group(segment, i, line_no, column_offset, line_src),
        b'[' => parse_list_literal(segment, i, line_no, column_offset, line_src),
        b'{' => parse_brace_literal(segment, i, line_no, column_offset, line_src),
        ch if is_ident_start(ch) => parse_ident(segment, i, line_no, column_offset, line_src),
        _ => Err(parse_error(
            line_no,
            column_offset + i,
            "Unsupported expression",
            line_src,
        )),
    }?;

    let bytes = segment.as_bytes();
    idx = skip_ws(bytes, idx);
    loop {
        if idx < bytes.len() && bytes[idx] == b'(' {
            let (new_expr, next) = parse_call(expr, segment, idx, line_no, column_offset, line_src)?;
            expr = new_expr;
            idx = skip_ws(bytes, next);
            continue;
        }
        if idx < bytes.len() && bytes[idx] == b'[' {
            let (new_expr, next) = parse_subscript(expr, segment, idx, line_no, column_offset, line_src)?;
            expr = new_expr;
            idx = skip_ws(bytes, next);
            continue;
        }
        if idx < bytes.len() && bytes[idx] == b'.' {
            let (new_expr, next) = parse_method_call(expr, segment, idx, line_no, column_offset, line_src)?;
            expr = new_expr;
            idx = skip_ws(bytes, next);
            continue;
        }
        break;
    }
    Ok((expr, idx))
}

fn parse_tuple_or_group(
    segment: &str,
    start: usize,
    line_no: usize,
    column_offset: usize,
    line_src: &str,
) -> Result<(Expr, usize), ParseError> {
    let bytes = segment.as_bytes();
    let i = skip_ws(bytes, start + 1);
    if i >= bytes.len() {
        return Err(parse_error(line_no, column_offset + start + 1, "Missing ')'", line_src));
    }
    if bytes[i] == b')' {
        return Ok((Expr::Tuple(Vec::new()), i + 1));
    }
    let (first, mut next) = parse_expression(segment, i, line_no, column_offset, line_src)?;
    next = skip_ws(bytes, next);
    if next < bytes.len() && bytes[next] == b',' {
        let mut items = vec![first];
        let mut idx = next + 1;
        loop {
            idx = skip_ws(bytes, idx);
            if idx >= bytes.len() {
                return Err(parse_error(line_no, column_offset + idx + 1, "Missing ')'", line_src));
            }
            if bytes[idx] == b')' {
                return Ok((Expr::Tuple(items), idx + 1));
            }
            let (expr, nxt) = parse_expression(segment, idx, line_no, column_offset, line_src)?;
            items.push(expr);
            idx = skip_ws(bytes, nxt);
            if idx < bytes.len() && bytes[idx] == b',' {
                idx += 1;
                continue;
            } else if idx < bytes.len() && bytes[idx] == b')' {
                return Ok((Expr::Tuple(items), idx + 1));
            } else {
                return Err(parse_error(
                    line_no,
                    column_offset + idx + 1,
                    "Expected ',' or ')'",
                    line_src,
                ));
            }
        }
    }
    if next < bytes.len() && bytes[next] == b')' {
        return Ok((first, next + 1));
    }
    Err(parse_error(
        line_no,
        column_offset + next + 1,
        "Missing ')'",
        line_src,
    ))
}

fn parse_list_literal(
    segment: &str,
    start: usize,
    line_no: usize,
    column_offset: usize,
    line_src: &str,
) -> Result<(Expr, usize), ParseError> {
    let bytes = segment.as_bytes();
    let mut i = skip_ws(bytes, start + 1);
    if i >= bytes.len() {
        return Err(parse_error(line_no, column_offset + i + 1, "Missing ']'", line_src));
    }
    if bytes[i] == b']' {
        return Ok((Expr::List(Vec::new()), i + 1));
    }

    let (first_expr, mut next) = parse_expression(segment, i, line_no, column_offset, line_src)?;
    next = skip_ws(bytes, next);
    // comprehension?
    let comp_check = skip_ws(bytes, next);
    if comp_check < bytes.len() && segment[comp_check..].starts_with("for") {
        let for_kw = comp_check;
        let after_for = for_kw + "for".len();
        let rest = &segment[after_for..];
        let in_pos = rest.find(" in ").ok_or_else(|| {
            parse_error(
                line_no,
                column_offset + after_for + rest.len(),
                "Expected 'in' in comprehension",
                line_src,
            )
        })?;
        let target_src = rest[..in_pos].trim();
        let target = parse_for_target(target_src, line_no, column_offset + after_for, line_src)?;
        let iter_start = after_for + in_pos + " in ".len();
        let (iter_expr, iter_end) = parse_expression(segment, iter_start, line_no, column_offset, line_src)?;
        let mut end_pos = skip_ws(bytes, iter_end);
        let mut cond = None;
        if end_pos < bytes.len() && segment[end_pos..].starts_with("if") {
            let cond_start = skip_ws(bytes, end_pos + "if".len());
            let (cond_expr, cond_end) = parse_expression(segment, cond_start, line_no, column_offset, line_src)?;
            cond = Some(Box::new(cond_expr));
            end_pos = skip_ws(bytes, cond_end);
        }
        if end_pos >= bytes.len() || bytes[end_pos] != b']' {
            return Err(parse_error(
                line_no,
                column_offset + end_pos + 1,
                "Missing ']' in list comprehension",
                line_src,
            ));
        }
        return Ok((
            Expr::ListComp {
                target,
                iter: Box::new(iter_expr),
                expr: Box::new(first_expr),
                cond,
            },
            end_pos + 1,
        ));
    }

    // regular list literal
    let mut items = vec![first_expr];
    i = next;
    loop {
        if i >= bytes.len() {
            return Err(parse_error(line_no, column_offset + i + 1, "Missing ']'", line_src));
        }
        if bytes[i] == b']' {
            return Ok((Expr::List(items), i + 1));
        }
        if bytes[i] == b',' {
            i = skip_ws(bytes, i + 1);
            if i >= bytes.len() {
                return Err(parse_error(line_no, column_offset + i + 1, "Missing ']'", line_src));
            }
            if bytes[i] == b']' {
                return Ok((Expr::List(items), i + 1));
            }
            let (expr, next) = parse_expression(segment, i, line_no, column_offset, line_src)?;
            items.push(expr);
            i = skip_ws(bytes, next);
            continue;
        }
        return Err(parse_error(
            line_no,
            column_offset + i + 1,
            "Expected ',' or ']'",
            line_src,
        ));
    }
}

fn parse_brace_literal(
    segment: &str,
    start: usize,
    line_no: usize,
    column_offset: usize,
    line_src: &str,
) -> Result<(Expr, usize), ParseError> {
    let bytes = segment.as_bytes();
    let mut i = skip_ws(bytes, start + 1);
    if i >= bytes.len() {
        return Err(parse_error(line_no, column_offset + i + 1, "Missing '}'", line_src));
    }
    if bytes[i] == b'}' {
        return Ok((Expr::Dict(Vec::new()), i + 1));
    }

    let (first_key, mut next) = parse_expression(segment, i, line_no, column_offset, line_src)?;
    next = skip_ws(bytes, next);
    if next < bytes.len() && bytes[next] == b':' {
        // dict literal
        let mut entries = Vec::new();
        i = next + 1;
        let (value, mut nxt) = parse_expression(segment, i, line_no, column_offset, line_src)?;
        entries.push((first_key, value));
        nxt = skip_ws(bytes, nxt);
        let comp_check = nxt;
        if comp_check < bytes.len() && segment[comp_check..].starts_with("for") {
            let after_for = comp_check + "for".len();
            let rest = &segment[after_for..];
            let in_pos = rest.find(" in ").ok_or_else(|| {
                parse_error(
                    line_no,
                    column_offset + after_for + rest.len(),
                    "Expected 'in' in comprehension",
                    line_src,
                )
            })?;
            let target_src = rest[..in_pos].trim();
            let target = parse_for_target(target_src, line_no, column_offset + after_for, line_src)?;
            let iter_start = after_for + in_pos + " in ".len();
            let (iter_expr, iter_end) = parse_expression(segment, iter_start, line_no, column_offset, line_src)?;
            let mut end_pos = skip_ws(bytes, iter_end);
            let mut cond = None;
            if end_pos < bytes.len() && segment[end_pos..].starts_with("if") {
                let cond_start = skip_ws(bytes, end_pos + "if".len());
                let (cond_expr, cond_end) = parse_expression(segment, cond_start, line_no, column_offset, line_src)?;
                cond = Some(Box::new(cond_expr));
                end_pos = skip_ws(bytes, cond_end);
            }
            if end_pos >= bytes.len() || bytes[end_pos] != b'}' {
                return Err(parse_error(
                    line_no,
                    column_offset + end_pos + 1,
                    "Missing '}' in dict comprehension",
                    line_src,
                ));
            }
            let (k, v) = entries.remove(0);
            return Ok((
                Expr::DictComp {
                    target,
                    iter: Box::new(iter_expr),
                    key: Box::new(k),
                    value: Box::new(v),
                    cond,
                },
                end_pos + 1,
            ));
        }
        if nxt < bytes.len() && bytes[nxt] == b',' {
            i = nxt + 1;
            loop {
                i = skip_ws(bytes, i);
                if i >= bytes.len() {
                    return Err(parse_error(line_no, column_offset + i + 1, "Missing '}'", line_src));
                }
                if bytes[i] == b'}' {
                    return Ok((Expr::Dict(entries), i + 1));
                }
                let (k, mut n2) = parse_expression(segment, i, line_no, column_offset, line_src)?;
                n2 = skip_ws(bytes, n2);
                if n2 >= bytes.len() || bytes[n2] != b':' {
                    return Err(parse_error(
                        line_no,
                        column_offset + n2 + 1,
                        "Expected ':' in dict literal",
                        line_src,
                    ));
                }
                let (v, mut n3) = parse_expression(segment, n2 + 1, line_no, column_offset, line_src)?;
                entries.push((k, v));
                n3 = skip_ws(bytes, n3);
                if n3 < bytes.len() && bytes[n3] == b',' {
                    i = n3 + 1;
                    continue;
                } else if n3 < bytes.len() && bytes[n3] == b'}' {
                    return Ok((Expr::Dict(entries), n3 + 1));
                } else {
                    return Err(parse_error(
                        line_no,
                        column_offset + n3 + 1,
                        "Expected ',' or '}' in dict literal",
                        line_src,
                    ));
                }
            }
        } else if nxt < bytes.len() && bytes[nxt] == b'}' {
            return Ok((Expr::Dict(entries), nxt + 1));
        }
        return Err(parse_error(
            line_no,
            column_offset + nxt + 1,
            "Expected ',' or '}' in dict literal",
            line_src,
        ));
    } else {
        // set literal
        let mut items = vec![first_key];
        i = next;
        loop {
            i = skip_ws(bytes, i);
            if i >= bytes.len() {
                return Err(parse_error(line_no, column_offset + i + 1, "Missing '}'", line_src));
            }
            if bytes[i] == b'}' {
                return Ok((Expr::Set(items), i + 1));
            }
            if bytes[i] == b',' {
                i += 1;
                i = skip_ws(bytes, i);
                if i < bytes.len() && bytes[i] == b'}' {
                    return Ok((Expr::Set(items), i + 1));
                }
                let (expr, nxt) = parse_expression(segment, i, line_no, column_offset, line_src)?;
                items.push(expr);
                i = skip_ws(bytes, nxt);
                continue;
            } else {
                return Err(parse_error(
                    line_no,
                    column_offset + i + 1,
                    "Expected ',' or '}' in set literal",
                    line_src,
                ));
            }
        }
    }
}

fn parse_subscript(
    object: Expr,
    segment: &str,
    open_idx: usize,
    line_no: usize,
    column_offset: usize,
    line_src: &str,
) -> Result<(Expr, usize), ParseError> {
    let bytes = segment.as_bytes();
    let (colon_pos, close_idx) = scan_subscript(segment, open_idx, line_no, column_offset, line_src)?;
    let content_start = skip_ws(bytes, open_idx + 1);
    if let Some(colon) = colon_pos {
        let mut start_expr = None;
        if content_start < colon {
            let (s, next) = parse_expression(segment, content_start, line_no, column_offset, line_src)?;
            let next = skip_ws(bytes, next);
            if next > colon {
                return Err(parse_error(
                    line_no,
                    column_offset + colon + 1,
                    "Invalid slice start",
                    line_src,
                ));
            }
            start_expr = Some(Box::new(s));
        }
        let mut end_expr = None;
        let end_start = skip_ws(bytes, colon + 1);
        if end_start < close_idx {
            let (e, next) = parse_expression(segment, end_start, line_no, column_offset, line_src)?;
            let next = skip_ws(bytes, next);
            if next > close_idx {
                return Err(parse_error(
                    line_no,
                    column_offset + close_idx + 1,
                    "Invalid slice end",
                    line_src,
                ));
            }
            end_expr = Some(Box::new(e));
        }
        Ok((
            Expr::Slice {
                value: Box::new(object),
                start: start_expr,
                end: end_expr,
            },
            close_idx + 1,
        ))
    } else {
        let (index_expr, next) = parse_expression(segment, content_start, line_no, column_offset, line_src)?;
        let next = skip_ws(bytes, next);
        if next != close_idx {
            return Err(parse_error(
                line_no,
                column_offset + close_idx + 1,
                "Unexpected trailing content in index",
                line_src,
            ));
        }
        Ok((Expr::Index(Box::new(object), Box::new(index_expr)), close_idx + 1))
    }
}

fn parse_method_call(
    object: Expr,
    segment: &str,
    dot_idx: usize,
    line_no: usize,
    column_offset: usize,
    line_src: &str,
) -> Result<(Expr, usize), ParseError> {
    let bytes = segment.as_bytes();
    let mut i = dot_idx + 1;
    if i >= bytes.len() || !is_ident_start(bytes[i]) {
        return Err(parse_error(
            line_no,
            column_offset + i + 1,
            "Expected method name",
            line_src,
        ));
    }
    let ident_start = i;
    i += 1;
    while i < bytes.len() && is_ident_part(bytes[i]) {
        i += 1;
    }
    let method = &segment[ident_start..i];
    i = skip_ws(bytes, i);
    if i < bytes.len() && bytes[i] == b'(' {
        // method call
        i += 1;
        i = skip_ws(bytes, i);
        if i >= bytes.len() {
            return Err(parse_error(line_no, column_offset + i + 1, "Missing ')'", line_src));
        }
        let mut arg = None;
        if bytes[i] != b')' {
            let (expr, next) = parse_expression(segment, i, line_no, column_offset, line_src)?;
            arg = Some(Box::new(expr));
            i = skip_ws(bytes, next);
            if i < bytes.len() && bytes[i] == b',' {
                return Err(parse_error(
                    line_no,
                    column_offset + i + 1,
                    "Only one argument supported in method calls",
                    line_src,
                ));
            }
        }
        if i >= bytes.len() || bytes[i] != b')' {
            return Err(parse_error(
                line_no,
                column_offset + i + 1,
                "Missing ')' after method call",
                line_src,
            ));
        }
        return Ok((
            Expr::MethodCall {
                object: Box::new(object),
                method: method.to_string(),
                arg,
            },
            i + 1,
        ));
    }
    Ok((
        Expr::Attr {
            object: Box::new(object),
            attr: method.to_string(),
        },
        i,
    ))
}

fn parse_call(
    callee: Expr,
    segment: &str,
    open_idx: usize,
    line_no: usize,
    column_offset: usize,
    line_src: &str,
) -> Result<(Expr, usize), ParseError> {
    let bytes = segment.as_bytes();
    let mut i = open_idx + 1;
    let mut args = Vec::new();
    loop {
        i = skip_ws(bytes, i);
        if i >= bytes.len() {
            return Err(parse_error(line_no, column_offset + i + 1, "Missing ')'", line_src));
        }
        if bytes[i] == b')' {
            return Ok((Expr::Call { callee: Box::new(callee), args }, i + 1));
        }
        if segment[i..].starts_with("key") {
            let mut k = i + 3;
            k = skip_ws(bytes, k);
            if k < bytes.len() && bytes[k] == b'=' {
                k += 1;
                k = skip_ws(bytes, k);
                let (arg, next) = parse_expression(segment, k, line_no, column_offset, line_src)?;
                args.push(arg);
                i = skip_ws(bytes, next);
                if i < bytes.len() && bytes[i] == b',' {
                    i += 1;
                    continue;
                } else if i < bytes.len() && bytes[i] == b')' {
                    return Ok((Expr::Call { callee: Box::new(callee), args }, i + 1));
                } else if i >= bytes.len() {
                    return Err(parse_error(line_no, column_offset + i + 1, "Missing ')'", line_src));
                }
                continue;
            }
        }
        let (arg, next) = parse_expression(segment, i, line_no, column_offset, line_src)?;
        args.push(arg);
        i = skip_ws(bytes, next);
        if i < bytes.len() && bytes[i] == b',' {
            i += 1;
            continue;
        } else if i < bytes.len() && bytes[i] == b')' {
            return Ok((Expr::Call { callee: Box::new(callee), args }, i + 1));
        } else {
            return Err(parse_error(
                line_no,
                column_offset + i + 1,
                "Expected ',' or ')'",
                line_src,
            ));
        }
    }
}

fn scan_subscript(
    segment: &str,
    open_idx: usize,
    line_no: usize,
    column_offset: usize,
    line_src: &str,
) -> Result<(Option<usize>, usize), ParseError> {
    let bytes = segment.as_bytes();
    let mut i = open_idx + 1;
    let mut depth = 0;
    let mut in_str = false;
    let mut colon = None;
    while i < bytes.len() {
        let ch = bytes[i];
        if in_str {
            if ch == b'\\' {
                i += 1;
            } else if ch == b'"' {
                in_str = false;
            }
        } else {
            match ch {
                b'"' => in_str = true,
                b'[' | b'{' | b'(' => depth += 1,
                b']' => {
                    if depth == 0 {
                        return Ok((colon, i));
                    }
                    depth -= 1;
                }
                b':' if depth == 0 => {
                    if colon.is_none() {
                        colon = Some(i);
                    }
                }
                _ => {}
            }
        }
        i += 1;
    }
    Err(parse_error(
        line_no,
        column_offset + open_idx + 1,
        "Missing ']'",
        line_src,
    ))
}

fn parse_string(
    segment: &str,
    start: usize,
    line_no: usize,
    column_offset: usize,
    line_src: &str,
) -> Result<(Expr, usize), ParseError> {
    let mut out = String::new();
    let bytes = segment.as_bytes();
    let mut i = start + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => {
                i += 1;
                if i >= bytes.len() {
                    return Err(parse_error(
                        line_no,
                        column_offset + i,
                        "Incomplete escape sequence",
                        line_src,
                    ));
                }
                let escaped = match bytes[i] {
                    b'\\' => '\\',
                    b'"' => '"',
                    b'n' => '\n',
                    b't' => '\t',
                    other => {
                        return Err(parse_error(
                            line_no,
                            column_offset + i + 1,
                            &format!("Unsupported escape: \\{}", other as char),
                            line_src,
                        ))
                    }
                };
                out.push(escaped);
            }
            b'"' => {
                return Ok((Expr::Str(out), i + 1));
            }
            ch => out.push(ch as char),
        }
        i += 1;
    }
    Err(parse_error(
        line_no,
        column_offset + start + 1,
        "Unterminated string literal",
        line_src,
    ))
}

fn parse_number(
    segment: &str,
    start: usize,
    line_no: usize,
    column_offset: usize,
    line_src: &str,
) -> Result<(Expr, usize), ParseError> {
    let bytes = segment.as_bytes();
    let mut i = start;
    let mut has_dot = false;
    while i < bytes.len() {
        let ch = bytes[i];
        if ch.is_ascii_digit() || ch == b'_' || (!has_dot && ch == b'.') {
            if ch == b'.' {
                has_dot = true;
            }
            i += 1;
            continue;
        }
        break;
    }
    let literal_raw = &segment[start..i];
    let literal: String = literal_raw.chars().filter(|c| *c != '_').collect();
    if has_dot {
        let value: f64 = literal.parse().map_err(|_| ParseError {
            message: "Float literal out of range".into(),
            line: line_no,
            column: column_offset + start,
            line_src: line_src.to_string(),
        })?;
        Ok((Expr::Float(value), i))
    } else {
        let value: i64 = literal.parse().map_err(|_| ParseError {
            message: "Integer literal out of range".into(),
            line: line_no,
            column: column_offset + start,
            line_src: line_src.to_string(),
        })?;
        Ok((Expr::Int(value), i))
    }
}

fn parse_ident(
    segment: &str,
    start: usize,
    _line_no: usize,
    _column_offset: usize,
    _line_src: &str,
) -> Result<(Expr, usize), ParseError> {
    let bytes = segment.as_bytes();
    let mut i = start + 1;
    while i < bytes.len() && is_ident_part(bytes[i]) {
        i += 1;
    }
    let ident = &segment[start..i];
    if ident == "True" {
        return Ok((Expr::Bool(true), i));
    }
    if ident == "False" {
        return Ok((Expr::Bool(false), i));
    }
    Ok((Expr::Var(ident.to_string()), i))
}

fn is_ident_start(ch: u8) -> bool {
    ch.is_ascii_alphabetic() || ch == b'_'
}

fn is_ident_part(ch: u8) -> bool {
    ch.is_ascii_alphanumeric() || ch == b'_'
}

fn is_valid_ident(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.is_empty() || !is_ident_start(bytes[0]) {
        return false;
    }
    for b in bytes.iter().skip(1) {
        if !is_ident_part(*b) {
            return false;
        }
    }
    true
}

fn skip_ws(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    i
}

// ----------- CODEGEN -------------

fn collect_targets_in_expr(expr: &Expr, set: &mut HashSet<String>) {
    match expr {
        Expr::List(items) | Expr::Tuple(items) | Expr::Set(items) => {
            for e in items {
                collect_targets_in_expr(e, set);
            }
        }
        Expr::Dict(entries) => {
            for (k, v) in entries {
                collect_targets_in_expr(k, set);
                collect_targets_in_expr(v, set);
            }
        }
        Expr::ListComp { target, iter, expr, cond } => {
            match target {
                ForTarget::Name(n) => {
                    set.insert(n.clone());
                }
                ForTarget::Tuple(names) => {
                    for n in names {
                        set.insert(n.clone());
                    }
                }
            }
            collect_targets_in_expr(iter, set);
            collect_targets_in_expr(expr, set);
            if let Some(c) = cond {
                collect_targets_in_expr(c, set);
            }
        }
        Expr::DictComp { target, iter, key, value, cond } => {
            match target {
                ForTarget::Name(n) => {
                    set.insert(n.clone());
                }
                ForTarget::Tuple(names) => {
                    for n in names {
                        set.insert(n.clone());
                    }
                }
            }
            collect_targets_in_expr(iter, set);
            collect_targets_in_expr(key, set);
            collect_targets_in_expr(value, set);
            if let Some(c) = cond {
                collect_targets_in_expr(c, set);
            }
        }
        Expr::Unary(_, e) | Expr::Lambda { body: e, .. } => collect_targets_in_expr(e, set),
        Expr::Binary(l, _, r) | Expr::Compare(l, _, r) | Expr::Index(l, r) => {
            collect_targets_in_expr(l, set);
            collect_targets_in_expr(r, set);
        }
        Expr::Call { callee, args } => {
            collect_targets_in_expr(callee, set);
            for a in args {
                collect_targets_in_expr(a, set);
            }
        }
        Expr::Slice { value, start, end } => {
            collect_targets_in_expr(value, set);
            if let Some(s) = start {
                collect_targets_in_expr(s, set);
            }
            if let Some(e) = end {
                collect_targets_in_expr(e, set);
            }
        }
        Expr::MethodCall { object, arg, .. } => {
            collect_targets_in_expr(object, set);
            if let Some(a) = arg {
                collect_targets_in_expr(a, set);
            }
        }
        Expr::Attr { object, .. } => {
            collect_targets_in_expr(object, set);
        }
        _ => {}
    }
}

fn collect_assigned_vars(program: &Program) -> Vec<String> {
    fn visit(stmts: &[Stmt], set: &mut HashSet<String>) {
        for stmt in stmts {
            match stmt {
                Stmt::Function { name, body, .. } => {
                    set.insert(name.clone());
                    visit(body, set);
                }
                Stmt::Class { name, methods } => {
                    set.insert(name.clone());
                    visit(methods, set);
                }
                Stmt::Assign { target, expr, .. } => {
                    match target {
                        AssignTarget::Name(n) => {
                            set.insert(n.clone());
                        }
                        AssignTarget::Subscript { object, .. } => {
                            if let Expr::Var(n) = object {
                                set.insert(n.clone());
                            }
                        }
                        AssignTarget::Attr { object, .. } => {
                            if let Expr::Var(n) = object {
                                set.insert(n.clone());
                            }
                        }
                        AssignTarget::Tuple { items } => {
                            for t in items {
                                match t {
                                    AssignTarget::Name(n) => {
                                        set.insert(n.clone());
                                    }
                                    AssignTarget::Subscript { object, .. } => {
                                        if let Expr::Var(n) = object {
                                            set.insert(n.clone());
                                        }
                                    }
                                    AssignTarget::Attr { object, .. } => {
                                        if let Expr::Var(n) = object {
                                            set.insert(n.clone());
                                        }
                                    }
                                    AssignTarget::Starred(n) => {
                                        set.insert(n.clone());
                                    }
                                    AssignTarget::Tuple { .. } => {}
                                }
                            }
                        }
                        AssignTarget::Starred(n) => {
                            set.insert(n.clone());
                        }
                    }
                    collect_targets_in_expr(expr, set);
                }
                Stmt::If { branches, else_branch } => {
                    for (cond, body) in branches {
                        collect_targets_in_expr(cond, set);
                        visit(body, set);
                    }
                    if let Some(body) = else_branch {
                        visit(body, set);
                    }
                }
                Stmt::While { cond, body } => {
                    collect_targets_in_expr(cond, set);
                    visit(body, set);
                }
                Stmt::For { target, iter, body } => {
                    collect_targets_in_expr(iter, set);
                    match target {
                        ForTarget::Name(n) => {
                            set.insert(n.clone());
                        }
                        ForTarget::Tuple(names) => {
                            for n in names {
                                set.insert(n.clone());
                            }
                        }
                    }
                    visit(body, set);
                }
                Stmt::With { target, expr, body } => {
                    set.insert(target.clone());
                    collect_targets_in_expr(expr, set);
                    visit(body, set);
                }
                Stmt::Try { body, handlers, finally_body } => {
                    visit(body, set);
                    for h in handlers {
                        if let Some(b) = &h.bind {
                            set.insert(b.clone());
                        }
                        visit(&h.body, set);
                    }
                    if let Some(f) = finally_body {
                        visit(f, set);
                    }
                }
                Stmt::Raise { expr } => {
                    collect_targets_in_expr(expr, set);
                }
                Stmt::Print { args } => {
                    for a in args {
                        collect_targets_in_expr(a, set);
                    }
                }
                Stmt::ExprStmt { expr } => collect_targets_in_expr(expr, set),
                Stmt::Return { expr } => {
                    if let Some(e) = expr {
                        collect_targets_in_expr(e, set);
                    }
                }
            }
        }
    }
    let mut set = HashSet::new();
    visit(&program.stmts, &mut set);
    let mut vars: Vec<_> = set.into_iter().collect();
    vars.sort();
    vars
}

fn collect_assigned_vars_body(body: &[Stmt]) -> Vec<String> {
    let mut set = HashSet::new();
    fn visit(stmts: &[Stmt], set: &mut HashSet<String>) {
        for stmt in stmts {
            match stmt {
                Stmt::Assign { target, expr, .. } => {
                    match target {
                        AssignTarget::Name(n) => {
                            set.insert(n.clone());
                        }
                        AssignTarget::Subscript { object, .. } => {
                            if let Expr::Var(n) = object {
                                set.insert(n.clone());
                            }
                        }
                        AssignTarget::Attr { object, .. } => {
                            if let Expr::Var(n) = object {
                                set.insert(n.clone());
                            }
                        }
                        AssignTarget::Tuple { items } => {
                            for t in items {
                                match t {
                                    AssignTarget::Name(n) => {
                                        set.insert(n.clone());
                                    }
                                    AssignTarget::Subscript { object, .. } => {
                                        if let Expr::Var(n) = object {
                                            set.insert(n.clone());
                                        }
                                    }
                                    AssignTarget::Attr { object, .. } => {
                                        if let Expr::Var(n) = object {
                                            set.insert(n.clone());
                                        }
                                    }
                                    AssignTarget::Starred(n) => {
                                        set.insert(n.clone());
                                    }
                                    AssignTarget::Tuple { .. } => {}
                                }
                            }
                        }
                        AssignTarget::Starred(n) => {
                            set.insert(n.clone());
                        }
                    }
                    collect_targets_in_expr(expr, set);
                }
                Stmt::If { branches, else_branch } => {
                    for (cond, b) in branches {
                        collect_targets_in_expr(cond, set);
                        visit(b, set);
                    }
                    if let Some(b) = else_branch {
                        visit(b, set);
                    }
                }
                Stmt::While { cond, body } => {
                    collect_targets_in_expr(cond, set);
                    visit(body, set);
                }
                Stmt::For { target, iter, body } => {
                    collect_targets_in_expr(iter, set);
                    match target {
                        ForTarget::Name(n) => {
                            set.insert(n.clone());
                        }
                        ForTarget::Tuple(names) => {
                            for n in names {
                                set.insert(n.clone());
                            }
                        }
                    }
                    visit(body, set);
                }
                Stmt::With { target, expr, body } => {
                    set.insert(target.clone());
                    collect_targets_in_expr(expr, set);
                    visit(body, set);
                }
                Stmt::Try { body, handlers, finally_body } => {
                    visit(body, set);
                    for h in handlers {
                        if let Some(b) = &h.bind {
                            set.insert(b.clone());
                        }
                        visit(&h.body, set);
                    }
                    if let Some(f) = finally_body {
                        visit(f, set);
                    }
                }
                Stmt::Raise { expr } => collect_targets_in_expr(expr, set),
                Stmt::Print { args } => {
                    for a in args {
                        collect_targets_in_expr(a, set);
                    }
                }
                Stmt::ExprStmt { expr } => collect_targets_in_expr(expr, set),
                Stmt::Return { expr } => {
                    if let Some(e) = expr {
                        collect_targets_in_expr(e, set);
                    }
                }
                Stmt::Function { .. } => {}
                Stmt::Class { name, methods } => {
                    set.insert(name.clone());
                    visit(methods, set);
                }
            }
        }
    }
    visit(body, &mut set);
    let mut vars: Vec<_> = set.into_iter().collect();
    vars.sort();
    vars
}

fn codegen_c(program: &Program) -> String {
    let mut cg = CodeGen::new();
    cg.emit_prelude();

    // collect functions (defs and lambdas)
    cg.register_functions(&program.stmts);

    // prototypes
    for f in &cg.functions {
        cg.out
            .push_str(&format!("Value {}(int argc, Value* args);\n", f.id));
    }
    cg.out.push_str("\n");

    // function definitions
    for f in cg.functions.clone() {
        cg.emit_function(&f);
    }

    // main
    cg.out.push_str("int main() {\n");
    let vars = collect_assigned_vars(program);
    for name in &vars {
        cg.push_indent(1);
        cg.out.push_str("Value ");
        cg.out.push_str(&cg.cname(name));
        cg.out.push_str(" = value_int(0);\n");
    }
    // bind named functions
    let funcs_clone = cg.functions.clone();
    for f in funcs_clone {
        if let Some(ref n) = f.name {
            cg.push_indent(1);
            cg.out
                .push_str(&format!("value_free({});\n", cg.cname(n)));
            cg.push_indent(1);
            cg.out
                .push_str(&format!("{} = value_func(&{});\n", cg.cname(n), f.id));
        }
    }

    cg.emit_block(&program.stmts, 1);

    for name in &vars {
        cg.push_indent(1);
        cg.out.push_str("value_free(");
        cg.out.push_str(&cg.cname(name));
        cg.out.push_str(");\n");
    }
    cg.push_indent(1);
    cg.out.push_str("return 0;\n}\n");
    cg.out
}

struct CodeGen {
    out: String,
    temp_counter: usize,
    functions: Vec<FunctionDef>,
    function_map: std::collections::HashMap<String, String>,
    lambda_map: std::collections::HashMap<String, String>,
    class_methods: std::collections::HashMap<String, Vec<(String, String)>>,
    return_ctx: Option<ReturnContext>,
}

#[derive(Clone)]
struct FunctionDef {
    id: String,
    name: Option<String>,
    params: Vec<String>,
    body: Vec<Stmt>,
}

struct ReturnContext {
    ret_var: String,
    label: String,
}

impl CodeGen {
    fn new() -> Self {
        Self {
            out: String::new(),
            temp_counter: 0,
            functions: Vec::new(),
            function_map: std::collections::HashMap::new(),
            lambda_map: std::collections::HashMap::new(),
            class_methods: std::collections::HashMap::new(),
            return_ctx: None,
        }
    }

    fn push_indent(&mut self, indent: usize) {
        for _ in 0..indent {
            self.out.push_str("    ");
        }
    }

    fn cname(&self, name: &str) -> String {
        format!("v_{}", name)
    }

    fn new_temp(&mut self) -> String {
        self.temp_counter += 1;
        format!("tmp{}", self.temp_counter)
    }

    fn new_sym(&mut self, prefix: &str) -> String {
        self.temp_counter += 1;
        format!("{}{}", prefix, self.temp_counter)
    }

    fn add_function(&mut self, name: Option<String>, params: Vec<String>, body: Vec<Stmt>) -> String {
        let id = self.new_sym("fn");
        if let Some(ref n) = name {
            self.function_map.insert(n.clone(), id.clone());
        }
        self.functions.push(FunctionDef { id: id.clone(), name, params, body });
        id
    }

    fn add_lambda(&mut self, key: String, params: Vec<String>, body: Vec<Stmt>) -> String {
        if let Some(existing) = self.lambda_map.get(&key) {
            return existing.clone();
        }
        let id = self.add_function(None, params, body);
        self.lambda_map.insert(key, id.clone());
        id
    }

    fn add_method(&mut self, class: &str, name: &str, params: Vec<String>, body: Vec<Stmt>) -> String {
        let id = self.add_function(None, params, body);
        self.class_methods.entry(class.to_string()).or_default().push((name.to_string(), id.clone()));
        id
    }

    fn emit_prelude(&mut self) {
        self.out.push_str(include_str!("prelude_c.txt"));
    }

    fn register_functions(&mut self, stmts: &[Stmt]) {
        for stmt in stmts {
            match stmt {
                Stmt::Function { name, params, body } => {
                    self.add_function(Some(name.clone()), params.clone(), body.clone());
                    self.register_functions(body);
                }
                Stmt::Class { name, methods } => {
                    for m in methods {
                        if let Stmt::Function { name: mname, params, body } = m {
                            self.add_method(name, mname, params.clone(), body.clone());
                            self.register_functions(body);
                        }
                    }
                }
                Stmt::If { branches, else_branch } => {
                    for (_, b) in branches {
                        self.register_functions(b);
                    }
                    if let Some(b) = else_branch {
                        self.register_functions(b);
                    }
                }
                Stmt::While { body, .. } => self.register_functions(body),
                Stmt::For { body, .. } => self.register_functions(body),
                Stmt::With { body, .. } => self.register_functions(body),
                Stmt::Try { body, handlers, finally_body } => {
                    self.register_functions(body);
                    for h in handlers {
                        self.register_functions(&h.body);
                    }
                    if let Some(f) = finally_body {
                        self.register_functions(f);
                    }
                }
                Stmt::ExprStmt { .. }
                | Stmt::Assign { .. }
                | Stmt::Print { .. } => self.register_exprs_in_stmt(stmt),
                Stmt::Return { expr } => {
                    if let Some(e) = expr {
                        self.register_functions_expr(e);
                    }
                }
                Stmt::Raise { expr } => self.register_functions_expr(expr),
            }
        }
    }

    fn register_exprs_in_stmt(&mut self, stmt: &Stmt) {
        match stmt {
            Stmt::Assign { expr, .. } => self.register_functions_expr(expr),
            Stmt::Print { args } => {
                for e in args {
                    self.register_functions_expr(e);
                }
            }
            Stmt::ExprStmt { expr } => self.register_functions_expr(expr),
            _ => {}
        }
    }

    fn register_functions_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Lambda { params, body } => {
                let func_body = vec![Stmt::Return { expr: Some((**body).clone()) }];
                let key = format!("lambda:{:?}", expr);
                self.add_lambda(key, params.clone(), func_body);
            }
            Expr::Unary(_, e) => self.register_functions_expr(e),
            Expr::MethodCall { object, arg, .. } => {
                self.register_functions_expr(object);
                if let Some(a) = arg {
                    self.register_functions_expr(a);
                }
            }
            Expr::Binary(l, _, r) | Expr::Compare(l, _, r) => {
                self.register_functions_expr(l);
                self.register_functions_expr(r);
            }
            Expr::Index(l, r) => {
                self.register_functions_expr(l);
                self.register_functions_expr(r);
            }
            Expr::Call { callee, args } => {
                self.register_functions_expr(callee);
                for a in args {
                    self.register_functions_expr(a);
                }
            }
            Expr::Slice { value, start, end } => {
                self.register_functions_expr(value);
                if let Some(s) = start {
                    self.register_functions_expr(s);
                }
                if let Some(e) = end {
                    self.register_functions_expr(e);
                }
            }
            Expr::List(items) | Expr::Tuple(items) | Expr::Set(items) => {
                for e in items {
                    self.register_functions_expr(e);
                }
            }
            Expr::ListComp { iter, expr, cond, .. } => {
                self.register_functions_expr(iter);
                self.register_functions_expr(expr);
                if let Some(c) = cond {
                    self.register_functions_expr(c);
                }
            }
            Expr::Dict(entries) => {
                for (k, v) in entries {
                    self.register_functions_expr(k);
                    self.register_functions_expr(v);
                }
            }
            Expr::DictComp { iter, key, value, cond, .. } => {
                self.register_functions_expr(iter);
                self.register_functions_expr(key);
                self.register_functions_expr(value);
                if let Some(c) = cond {
                    self.register_functions_expr(c);
                }
            }
            _ => {}
        }
    }

    fn emit_function(&mut self, def: &FunctionDef) {
        self.out
            .push_str(&format!("Value {}(int argc, Value* args) {{\n", def.id));
        let ret_var = self.new_sym("ret");
        self.push_indent(1);
        self.out
            .push_str(&format!("Value {} = value_int(0);\n", ret_var));
        self.push_indent(1);
        self.out.push_str(&format!(
            "if (argc != {}) {{ fprintf(stderr, \"wrong number of args\\n\"); exit(1); }}\n",
            def.params.len()
        ));
        let mut assigned = collect_assigned_vars_body(&def.body);
        for p in &def.params {
            if !assigned.contains(p) {
                assigned.push(p.clone());
            }
        }
        assigned.sort();
        assigned.dedup();
        for name in &assigned {
            self.push_indent(1);
            self.out
                .push_str(&format!("Value {} = value_int(0);\n", self.cname(name)));
        }
        for (idx, p) in def.params.iter().enumerate() {
            self.push_indent(1);
            self.out
                .push_str(&format!("value_free({});\n", self.cname(p)));
            self.push_indent(1);
            self.out
                .push_str(&format!("{} = value_clone(args[{}]);\n", self.cname(p), idx));
        }
        let prev_return = self.return_ctx.replace(ReturnContext {
            ret_var: ret_var.clone(),
            label: format!("{}_ret", def.id),
        });
        self.emit_block(&def.body, 1);
        if let Some(prev) = prev_return {
            self.return_ctx = Some(prev);
        } else {
            self.return_ctx = None;
        }
        self.push_indent(1);
        self.out.push_str(&format!("goto {};\n", format!("{}_ret", def.id)));
        self.out.push_str(&format!("{}:\n", format!("{}_ret", def.id)));
        for name in &assigned {
            self.push_indent(1);
            self.out
                .push_str(&format!("value_free({});\n", self.cname(name)));
        }
        self.push_indent(1);
        self.out
            .push_str(&format!("return {};\n", ret_var));
        self.out.push_str("}\n\n");
    }

    fn emit_block(&mut self, stmts: &[Stmt], indent: usize) {
        for stmt in stmts {
            self.emit_stmt(stmt, indent);
        }
    }

    fn emit_stmt(&mut self, stmt: &Stmt, indent: usize) {
        match stmt {
            Stmt::Function { name, .. } => {
                if let Some(id_val) = self.function_map.get(name).cloned() {
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("value_free({});\n", self.cname(name)));
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("{} = value_func(&{});\n", self.cname(name), id_val));
                }
            }
            Stmt::Class { name, .. } => {
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", self.cname(name)));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("{} = value_class(\"{}\");\n", self.cname(name), name));
                if let Some(methods) = self.class_methods.get(name).cloned() {
                    for (mname, fid) in methods {
                        let tmp = self.new_temp();
                        self.push_indent(indent);
                        self.out
                            .push_str(&format!("Value {} = value_func(&{});\n", tmp, fid));
                        self.push_indent(indent);
                        self.out.push_str(&format!(
                            "class_add_method({}, \"{}\", {});\n",
                            self.cname(name),
                            mname,
                            tmp
                        ));
                        self.push_indent(indent);
                        self.out
                            .push_str(&format!("value_free({});\n", tmp));
                    }
                }
            }
            Stmt::Assign { target, expr, op } => self.emit_assign(target, expr, *op, indent),
            Stmt::Print { args } => {
                for (idx, arg) in args.iter().enumerate() {
                    let tmp = self.new_temp();
                    self.emit_expr(arg, &tmp, indent);
                    self.push_indent(indent);
                    self.out.push_str("value_print(");
                    self.out.push_str(&tmp);
                    self.out.push_str(");\n");
                    self.push_indent(indent);
                    self.out.push_str("value_free(");
                    self.out.push_str(&tmp);
                    self.out.push_str(");\n");
                    if idx + 1 < args.len() {
                        self.push_indent(indent);
                        self.out.push_str("printf(\" \");\n");
                    }
                }
                self.push_indent(indent);
                self.out.push_str("printf(\"\\n\");\n");
            }
            Stmt::ExprStmt { expr } => {
                let tmp = self.new_temp();
                self.emit_expr(expr, &tmp, indent);
                self.push_indent(indent);
                self.out.push_str(&format!("value_free({});\n", tmp));
            }
            Stmt::If { branches, else_branch } => {
                self.emit_if(branches, else_branch.as_ref(), indent);
            }
            Stmt::While { cond, body } => {
                self.push_indent(indent);
                self.out.push_str("while (1) {\n");
                let cond_tmp = self.new_temp();
                self.emit_expr(cond, &cond_tmp, indent + 1);
                self.push_indent(indent + 1);
                self.out
                    .push_str(&format!("if (!value_truthy({})) {{\n", cond_tmp));
                self.push_indent(indent + 2);
                self.out
                    .push_str(&format!("value_free({});\n", cond_tmp));
                self.push_indent(indent + 2);
                self.out.push_str("break;\n");
                self.push_indent(indent + 1);
                self.out.push_str("}\n");
                self.push_indent(indent + 1);
                self.out
                    .push_str(&format!("value_free({});\n", cond_tmp));
                self.emit_block(body, indent + 1);
                self.push_indent(indent);
                self.out.push_str("}\n");
            }
            Stmt::For { target, iter, body } => {
                let iter_tmp = self.new_temp();
                self.emit_expr(iter, &iter_tmp, indent);
                let list_tmp = self.new_temp();
                self.push_indent(indent);
                self.out
                    .push_str(&format!("Value {} = value_iterable_to_list({});\n", list_tmp, iter_tmp));
                self.push_indent(indent);
                self.out.push_str(&format!("value_free({});\n", iter_tmp));
                let idx_name = self.new_sym("i");
                self.push_indent(indent);
                self.out.push_str(&format!(
                    "for (size_t {idx}=0; {idx} < {list}.as.list->len; {idx}++) {{\n",
                    idx = idx_name,
                    list = list_tmp
                ));
                let item_tmp = self.new_temp();
                self.push_indent(indent + 1);
                self.out.push_str(&format!(
                    "Value {} = value_clone({}.as.list->items[{}]);\n",
                    item_tmp, list_tmp, idx_name
                ));
                self.emit_for_target_bind(target, &item_tmp, indent + 1);
                self.push_indent(indent + 1);
                self.out
                    .push_str(&format!("value_free({});\n", item_tmp));
                self.emit_block(body, indent + 1);
                self.push_indent(indent);
                self.out.push_str("}\n");
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", list_tmp));
            }
            Stmt::With { target, expr, body } => {
                let ctx_tmp = self.new_temp();
                self.emit_expr(expr, &ctx_tmp, indent);
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", self.cname(target)));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("{} = value_clone({});\n", self.cname(target), ctx_tmp));
                self.emit_block(body, indent + 1);
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_with_close({});\n", ctx_tmp));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", ctx_tmp));
            }
            Stmt::Try { body, handlers, finally_body } => {
                let frame = self.new_sym("exc_frame");
                let flag = self.new_sym("exc_flag");
                let handled = self.new_sym("exc_handled");
                self.push_indent(indent);
                self.out
                    .push_str(&format!("ExcFrame {};\n", frame));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("exc_push(&{});\n", frame));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("int {} = setjmp({}.buf);\n", flag, frame));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("int {} = 0;\n", handled));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("if (!{}) {{\n", flag));
                self.emit_block(body, indent + 1);
                self.push_indent(indent);
                self.out.push_str("}\n");
                self.push_indent(indent);
                self.out
                    .push_str(&format!("exc_pop(&{});\n", frame));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("if ({}) {{\n", flag));
                for h in handlers {
                    self.push_indent(indent + 1);
                    self.out.push_str(&format!(
                        "if (!{} && exc_match_name({})) {{\n",
                        handled,
                        h.name
                            .as_ref()
                            .map(|n| format!("\"{}\"", n))
                            .unwrap_or_else(|| "NULL".to_string())
                    ));
                    if let Some(bind) = &h.bind {
                        self.push_indent(indent + 2);
                        self.out
                            .push_str(&format!("value_free({});\n", self.cname(bind)));
                        self.push_indent(indent + 2);
                        self.out
                            .push_str(&format!("{} = exc_get();\n", self.cname(bind)));
                    }
                    self.push_indent(indent + 2);
                    self.out
                        .push_str(&format!("{} = 1;\n", handled));
                    self.push_indent(indent + 2);
                    self.out.push_str("exc_clear();\n");
                    self.emit_block(&h.body, indent + 2);
                    self.push_indent(indent + 1);
                    self.out.push_str("}\n");
                }
                self.push_indent(indent);
                self.out.push_str("}\n");
                if let Some(f) = finally_body {
                    self.push_indent(indent);
                    self.out.push_str("{\n");
                    self.emit_block(f, indent + 1);
                    self.push_indent(indent);
                    self.out.push_str("}\n");
                }
                self.push_indent(indent);
                self.out
                    .push_str(&format!("if ({} && !{}) exc_rethrow();\n", flag, handled));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("if ({} && {}) exc_clear();\n", flag, handled));
            }
            Stmt::Return { expr } => {
                if let Some(ctx) = &self.return_ctx {
                    let ret_var = ctx.ret_var.clone();
                    let label = ctx.label.clone();
                    if let Some(e) = expr {
                        let tmp = self.new_temp();
                        self.emit_expr(e, &tmp, indent);
                        self.push_indent(indent);
                        self.out
                            .push_str(&format!("value_free({});\n", ret_var));
                        self.push_indent(indent);
                        self.out
                            .push_str(&format!("{} = {};\n", ret_var, tmp));
                    } else {
                        let tmp = self.new_temp();
                        self.push_indent(indent);
                        self.out
                            .push_str(&format!("Value {} = value_int(0);\n", tmp));
                        self.push_indent(indent);
                        self.out
                            .push_str(&format!("value_free({});\n", ret_var));
                        self.push_indent(indent);
                        self.out
                            .push_str(&format!("{} = {};\n", ret_var, tmp));
                    }
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("goto {};\n", label));
                } else {
                    if let Some(e) = expr {
                        let tmp = self.new_temp();
                        self.emit_expr(e, &tmp, indent);
                        self.push_indent(indent);
                        self.out
                            .push_str(&format!("value_free({});\n", tmp));
                    }
                }
            }
            Stmt::Raise { expr } => {
                let tmp = self.new_temp();
                self.emit_expr(expr, &tmp, indent);
                self.push_indent(indent);
                self.out
                    .push_str(&format!("exc_raise({});\n", tmp));
            }
        }
    }

    fn emit_if(&mut self, branches: &[(Expr, Vec<Stmt>)], else_branch: Option<&Vec<Stmt>>, indent: usize) {
        if branches.is_empty() {
            if let Some(body) = else_branch {
                self.emit_block(body, indent);
            }
            return;
        }
        let (cond, body) = &branches[0];
        let cond_tmp = self.new_temp();
        self.emit_expr(cond, &cond_tmp, indent);
        self.push_indent(indent);
        self.out
            .push_str(&format!("if (value_truthy({})) {{\n", cond_tmp));
        self.push_indent(indent + 1);
        self.out
            .push_str(&format!("value_free({});\n", cond_tmp));
        self.emit_block(body, indent + 1);
        self.push_indent(indent);
        self.out.push_str("} else {\n");
        self.push_indent(indent + 1);
        self.out
            .push_str(&format!("value_free({});\n", cond_tmp));
        if branches.len() > 1 {
            self.emit_if(&branches[1..], else_branch, indent + 1);
        } else if let Some(body) = else_branch {
            self.emit_block(body, indent + 1);
        }
        self.push_indent(indent);
        self.out.push_str("}\n");
    }

    fn emit_assign(&mut self, target: &AssignTarget, expr: &Expr, op: AssignOp, indent: usize) {
        match target {
            AssignTarget::Name(name) => {
                if matches!(op, AssignOp::Assign) {
                    let tmp = self.new_temp();
                    self.emit_expr(expr, &tmp, indent);
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("value_free({});\n", self.cname(name)));
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("{} = {};\n", self.cname(name), tmp));
                } else {
                    let lhs_tmp = self.new_temp();
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("Value {} = value_clone({});\n", lhs_tmp, self.cname(name)));
                    let rhs_tmp = self.new_temp();
                    self.emit_expr(expr, &rhs_tmp, indent);
                    let func = match op {
                        AssignOp::Add => "value_add",
                        AssignOp::Sub => "value_sub",
                        AssignOp::Mul => "value_mul",
                        AssignOp::Div => "value_div",
                        AssignOp::Assign => unreachable!(),
                    };
                    let res_tmp = self.new_temp();
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("Value {} = {}({}, {});\n", res_tmp, func, lhs_tmp, rhs_tmp));
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("value_free({});\n", self.cname(name)));
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("{} = {};\n", self.cname(name), res_tmp));
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("value_free({});\n", lhs_tmp));
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("value_free({});\n", rhs_tmp));
                }
            }
            AssignTarget::Subscript { object, index } => {
                let container_name = match object {
                    Expr::Var(ref n) => n,
                    _ => unreachable!(),
                };
                let idx_tmp = self.new_temp();
                self.emit_expr(index, &idx_tmp, indent);
                if matches!(op, AssignOp::Assign) {
                    let val_tmp = self.new_temp();
                    self.emit_expr(expr, &val_tmp, indent);
                    self.push_indent(indent);
                    self.out.push_str(&format!(
                        "value_set_item({}, {}, {});\n",
                        self.cname(container_name), idx_tmp, val_tmp
                    ));
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("value_free({});\n", idx_tmp));
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("value_free({});\n", val_tmp));
                } else {
                    let cur_tmp = self.new_temp();
                    self.push_indent(indent);
                    self.out.push_str(&format!(
                        "Value {} = value_index({}, {});\n",
                        cur_tmp, self.cname(container_name), idx_tmp
                    ));
                    let rhs_tmp = self.new_temp();
                    self.emit_expr(expr, &rhs_tmp, indent);
                    let func = match op {
                        AssignOp::Add => "value_add",
                        AssignOp::Sub => "value_sub",
                        AssignOp::Mul => "value_mul",
                        AssignOp::Div => "value_div",
                        AssignOp::Assign => unreachable!(),
                    };
                    let res_tmp = self.new_temp();
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("Value {} = {}({}, {});\n", res_tmp, func, cur_tmp, rhs_tmp));
                    self.push_indent(indent);
                    self.out.push_str(&format!(
                        "value_set_item({}, {}, {});\n",
                        self.cname(container_name), idx_tmp, res_tmp
                    ));
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("value_free({});\n", idx_tmp));
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("value_free({});\n", cur_tmp));
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("value_free({});\n", rhs_tmp));
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("value_free({});\n", res_tmp));
                }
            }
            AssignTarget::Attr { object, attr } => {
                let obj_tmp = self.new_temp();
                self.emit_expr(object, &obj_tmp, indent);
                if matches!(op, AssignOp::Assign) {
                    let val_tmp = self.new_temp();
                    self.emit_expr(expr, &val_tmp, indent);
                    self.push_indent(indent);
                    self.out.push_str(&format!(
                        "value_set_attr({}, \"{}\", {});\n",
                        obj_tmp, attr, val_tmp
                    ));
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("value_free({});\n", obj_tmp));
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("value_free({});\n", val_tmp));
                } else {
                    let cur_tmp = self.new_temp();
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("Value {} = value_get_attr({}, \"{}\");\n", cur_tmp, obj_tmp, attr));
                    let rhs_tmp = self.new_temp();
                    self.emit_expr(expr, &rhs_tmp, indent);
                    let func = match op {
                        AssignOp::Add => "value_add",
                        AssignOp::Sub => "value_sub",
                        AssignOp::Mul => "value_mul",
                        AssignOp::Div => "value_div",
                        AssignOp::Assign => unreachable!(),
                    };
                    let res_tmp = self.new_temp();
                    self.push_indent(indent);
                    self.out.push_str(&format!(
                        "Value {} = {}({}, {});\n",
                        res_tmp, func, cur_tmp, rhs_tmp
                    ));
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("value_set_attr({}, \"{}\", {});\n", obj_tmp, attr, res_tmp));
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("value_free({});\n", obj_tmp));
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("value_free({});\n", cur_tmp));
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("value_free({});\n", rhs_tmp));
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("value_free({});\n", res_tmp));
                }
            }
            AssignTarget::Tuple { items } => {
                let src_tmp = self.new_temp();
                self.emit_expr(expr, &src_tmp, indent);
                let list_tmp = self.new_temp();
                self.push_indent(indent);
                self.out
                    .push_str(&format!("Value {} = value_iterable_to_list({});\n", list_tmp, src_tmp));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", src_tmp));
                let len_sym = self.new_sym("len");
                self.push_indent(indent);
                self.out
                    .push_str(&format!("size_t {} = {}.as.list ? {}.as.list->len : 0;\n", len_sym, list_tmp, list_tmp));
                let mut star_idx = None;
                for (idx, it) in items.iter().enumerate() {
                    if let AssignTarget::Starred(_) = it {
                        star_idx = Some(idx);
                        break;
                    }
                }
                if let Some(sidx) = star_idx {
                    let req = items.len().saturating_sub(1);
                    self.push_indent(indent);
                    self.out.push_str(&format!(
                        "if ({} < {}) {{ fprintf(stderr, \"unpack error: not enough values\\n\"); exit(1); }}\n",
                        len_sym, req
                    ));
                    let after = items.len() - sidx - 1;
                    for (idx, it) in items.iter().enumerate() {
                        match it {
                            AssignTarget::Starred(name) => {
                                let start_tmp = self.new_temp();
                                let end_tmp = self.new_temp();
                                self.push_indent(indent);
                                self.out
                                    .push_str(&format!("Value {} = value_int({});\n", start_tmp, idx));
                                self.push_indent(indent);
                                self.out.push_str(&format!(
                                    "Value {} = value_int((long long){} - {} + {});\n",
                                    end_tmp, len_sym, after, idx
                                ));
                                self.push_indent(indent);
                                self.out.push_str(&format!(
                                    "Value tmp_slice = value_slice({}, &{}, 1, &{}, 1);\n",
                                    list_tmp, start_tmp, end_tmp
                                ));
                                self.push_indent(indent);
                                self.out
                                    .push_str(&format!("value_free({});\n", start_tmp));
                                self.push_indent(indent);
                                self.out
                                    .push_str(&format!("value_free({});\n", end_tmp));
                                self.push_indent(indent);
                                self.out
                                    .push_str(&format!("value_free({});\n", self.cname(name)));
                                self.push_indent(indent);
                                self.out
                                    .push_str(&format!("{} = tmp_slice;\n", self.cname(name)));
                            }
                            _ => {
                                let idx_val = if idx < sidx {
                                    format!("{}", idx)
                                } else {
                                    let suffix_pos = idx - sidx - 1;
                                    format!("({} - {} + {})", len_sym, after, suffix_pos)
                                };
                                let item_tmp = self.new_temp();
                                self.push_indent(indent);
                                self.out.push_str(&format!(
                                    "Value {} = value_clone({}.as.list->items[{}]);\n",
                                    item_tmp, list_tmp, idx_val
                                ));
                                self.assign_to_target(it, &item_tmp, indent);
                                self.push_indent(indent);
                                self.out
                                    .push_str(&format!("value_free({});\n", item_tmp));
                            }
                        }
                    }
                } else {
                    self.push_indent(indent);
                    self.out.push_str(&format!(
                        "if ({} != {}) {{ fprintf(stderr, \"unpack error: expected {} values\\n\"); exit(1); }}\n",
                        len_sym,
                        items.len(),
                        items.len()
                    ));
                    for (idx, it) in items.iter().enumerate() {
                        let item_tmp = self.new_temp();
                        self.push_indent(indent);
                        self.out.push_str(&format!(
                            "Value {} = value_clone({}.as.list->items[{}]);\n",
                            item_tmp, list_tmp, idx
                        ));
                        self.assign_to_target(it, &item_tmp, indent);
                        self.push_indent(indent);
                        self.out
                            .push_str(&format!("value_free({});\n", item_tmp));
                    }
                }
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", list_tmp));
            }
            AssignTarget::Starred(name) => {
                let tmp = self.new_temp();
                self.emit_expr(expr, &tmp, indent);
                let list_tmp = self.new_temp();
                self.push_indent(indent);
                self.out
                    .push_str(&format!("Value {} = value_iterable_to_list({});\n", list_tmp, tmp));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", tmp));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", self.cname(name)));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("{} = {};\n", self.cname(name), list_tmp));
            }
        }
    }

    fn assign_to_target(&mut self, target: &AssignTarget, value_tmp: &str, indent: usize) {
        match target {
            AssignTarget::Name(name) => {
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", self.cname(name)));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("{} = value_clone({});\n", self.cname(name), value_tmp));
            }
            AssignTarget::Subscript { object, index } => {
                let container_name = match object {
                    Expr::Var(ref n) => n,
                    _ => unreachable!(),
                };
                let idx_tmp = self.new_temp();
                self.emit_expr(index, &idx_tmp, indent);
                self.push_indent(indent);
                self.out.push_str(&format!(
                    "value_set_item({}, {}, {});\n",
                    self.cname(container_name), idx_tmp, value_tmp
                ));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", idx_tmp));
            }
            AssignTarget::Starred(name) => {
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", self.cname(name)));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("{} = value_clone({});\n", self.cname(name), value_tmp));
            }
            AssignTarget::Attr { object, attr } => {
                let obj_tmp = self.new_temp();
                self.emit_expr(object, &obj_tmp, indent);
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_set_attr({}, \"{}\", {});\n", obj_tmp, attr, value_tmp));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", obj_tmp));
            }
            AssignTarget::Tuple { .. } => {}
        }
    }

    fn emit_for_target_bind(&mut self, target: &ForTarget, value_tmp: &str, indent: usize) {
        match target {
            ForTarget::Name(name) => {
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", self.cname(name)));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("{} = value_clone({});\n", self.cname(name), value_tmp));
            }
            ForTarget::Tuple(names) => {
                self.push_indent(indent);
                self.out.push_str(&format!(
                    "if ({}.tag != VAL_TUPLE || {}.as.tuple->len < {}) {{ fprintf(stderr, \"unpack error in for tuple\\n\"); exit(1); }}\n",
                    value_tmp,
                    value_tmp,
                    names.len()
                ));
                for (idx, name) in names.iter().enumerate() {
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("value_free({});\n", self.cname(name)));
                    self.push_indent(indent);
                    self.out.push_str(&format!(
                        "{} = value_clone({}.as.tuple->items[{}]);\n",
                        self.cname(name), value_tmp, idx
                    ));
                }
            }
        }
    }

    fn emit_expr(&mut self, expr: &Expr, target: &str, indent: usize) {
        match expr {
            Expr::Int(v) => {
                self.push_indent(indent);
                self.out
                    .push_str(&format!("Value {} = value_int({});\n", target, v));
            }
            Expr::Float(v) => {
                self.push_indent(indent);
                self.out
                    .push_str(&format!("Value {} = value_float({});\n", target, v));
            }
            Expr::Str(s) => {
                self.push_indent(indent);
                self.out.push_str(&format!(
                    "Value {} = value_str(\"{}\");\n",
                    target,
                    escape_c_string(s)
                ));
            }
            Expr::Bool(b) => {
                self.push_indent(indent);
                self.out.push_str(&format!(
                    "Value {} = value_bool({});\n",
                    target,
                    if *b { 1 } else { 0 }
                ));
            }
            Expr::Lambda { .. } => {
                let key = format!("lambda:{:?}", expr);
                if let Some(id) = self.lambda_map.get(&key).cloned() {
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("Value {} = value_func(&{});\n", target, id));
                } else {
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("Value {} = value_str(\"<lambda missing>\");\n", target));
                }
            }
            Expr::List(items) => {
                if items.is_empty() {
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("Value {} = value_list_literal(0, NULL);\n", target));
                } else {
                    let mut temps = Vec::new();
                    for item in items {
                        let t = self.new_temp();
                        self.emit_expr(item, &t, indent);
                        temps.push(t);
                    }
                    let arr_name = self.new_sym("arr");
                    self.push_indent(indent);
                    self.out.push_str(&format!("Value {}[] = {{", arr_name));
                    for (idx, t) in temps.iter().enumerate() {
                        if idx > 0 {
                            self.out.push_str(", ");
                        }
                        self.out.push_str(t);
                    }
                    self.out.push_str("};\n");
                    self.push_indent(indent);
                    self.out.push_str(&format!(
                        "Value {} = value_list_literal({}, {});\n",
                        target,
                        temps.len(),
                        arr_name
                    ));
                    for t in temps {
                        self.push_indent(indent);
                        self.out
                            .push_str(&format!("value_free({});\n", t));
                    }
                }
            }
            Expr::Tuple(items) => {
                if items.is_empty() {
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("Value {} = value_tuple_literal(0, NULL);\n", target));
                } else {
                    let mut temps = Vec::new();
                    for item in items {
                        let t = self.new_temp();
                        self.emit_expr(item, &t, indent);
                        temps.push(t);
                    }
                    let arr_name = self.new_sym("arr");
                    self.push_indent(indent);
                    self.out.push_str(&format!("Value {}[] = {{", arr_name));
                    for (idx, t) in temps.iter().enumerate() {
                        if idx > 0 {
                            self.out.push_str(", ");
                        }
                        self.out.push_str(t);
                    }
                    self.out.push_str("};\n");
                    self.push_indent(indent);
                    self.out.push_str(&format!(
                        "Value {} = value_tuple_literal({}, {});\n",
                        target,
                        temps.len(),
                        arr_name
                    ));
                    for t in temps {
                        self.push_indent(indent);
                        self.out
                            .push_str(&format!("value_free({});\n", t));
                    }
                }
            }
            Expr::Dict(entries) => {
                if entries.is_empty() {
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("Value {} = value_dict_literal(0, NULL, NULL);\n", target));
                } else {
                    let mut key_tmps = Vec::new();
                    let mut val_tmps = Vec::new();
                    for (k, v) in entries {
                        let kt = self.new_temp();
                        let vt = self.new_temp();
                        self.emit_expr(k, &kt, indent);
                        self.emit_expr(v, &vt, indent);
                        key_tmps.push(kt);
                        val_tmps.push(vt);
                    }
                    let keys_arr = self.new_sym("keys");
                    let vals_arr = self.new_sym("vals");
                    self.push_indent(indent);
                    self.out.push_str(&format!("Value {}[] = {{", keys_arr));
                    for (idx, t) in key_tmps.iter().enumerate() {
                        if idx > 0 {
                            self.out.push_str(", ");
                        }
                        self.out.push_str(t);
                    }
                    self.out.push_str("};\n");
                    self.push_indent(indent);
                    self.out.push_str(&format!("Value {}[] = {{", vals_arr));
                    for (idx, t) in val_tmps.iter().enumerate() {
                        if idx > 0 {
                            self.out.push_str(", ");
                        }
                        self.out.push_str(t);
                    }
                    self.out.push_str("};\n");
                    self.push_indent(indent);
                    self.out.push_str(&format!(
                        "Value {} = value_dict_literal({}, {}, {});\n",
                        target,
                        key_tmps.len(),
                        keys_arr,
                        vals_arr
                    ));
                    for t in key_tmps {
                        self.push_indent(indent);
                        self.out
                            .push_str(&format!("value_free({});\n", t));
                    }
                    for t in val_tmps {
                        self.push_indent(indent);
                        self.out
                            .push_str(&format!("value_free({});\n", t));
                    }
                }
            }
            Expr::Set(items) => {
                if items.is_empty() {
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("Value {} = value_set_literal(0, NULL);\n", target));
                } else {
                    let mut temps = Vec::new();
                    for item in items {
                        let t = self.new_temp();
                        self.emit_expr(item, &t, indent);
                        temps.push(t);
                    }
                    let arr_name = self.new_sym("arr");
                    self.push_indent(indent);
                    self.out.push_str(&format!("Value {}[] = {{", arr_name));
                    for (idx, t) in temps.iter().enumerate() {
                        if idx > 0 {
                            self.out.push_str(", ");
                        }
                        self.out.push_str(t);
                    }
                    self.out.push_str("};\n");
                    self.push_indent(indent);
                    self.out.push_str(&format!(
                        "Value {} = value_set_literal({}, {});\n",
                        target,
                        temps.len(),
                        arr_name
                    ));
                    for t in temps {
                        self.push_indent(indent);
                        self.out
                            .push_str(&format!("value_free({});\n", t));
                    }
                }
            }
            Expr::ListComp { target: for_target, iter, expr, cond } => {
                self.push_indent(indent);
                self.out
                    .push_str(&format!("Value {} = value_list_literal(0, NULL);\n", target));
                let iter_tmp = self.new_temp();
                self.emit_expr(iter, &iter_tmp, indent);
                let list_tmp = self.new_temp();
                self.push_indent(indent);
                self.out.push_str(&format!(
                    "Value {} = value_iterable_to_list({});\n",
                    list_tmp, iter_tmp
                ));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", iter_tmp));
                let idx = self.new_sym("i");
                self.push_indent(indent);
                self.out.push_str(&format!(
                    "for (size_t {idx}=0; {idx} < {list}.as.list->len; {idx}++) {{\n",
                    idx = idx,
                    list = list_tmp
                ));
                let item_tmp = self.new_temp();
                self.push_indent(indent + 1);
                self.out.push_str(&format!(
                    "Value {} = value_clone({}.as.list->items[{}]);\n",
                    item_tmp, list_tmp, idx
                ));
                self.emit_for_target_bind(for_target, &item_tmp, indent + 1);
                self.push_indent(indent + 1);
                self.out
                    .push_str(&format!("value_free({});\n", item_tmp));
                if let Some(c) = cond {
                    let cond_tmp = self.new_temp();
                    self.emit_expr(c, &cond_tmp, indent + 1);
                    self.push_indent(indent + 1);
                    self.out.push_str(&format!(
                        "if (!value_truthy({})) {{ value_free({}); continue; }}\n",
                        cond_tmp, cond_tmp
                    ));
                    self.push_indent(indent + 1);
                    self.out
                        .push_str(&format!("value_free({});\n", cond_tmp));
                }
                let val_tmp = self.new_temp();
                self.emit_expr(expr, &val_tmp, indent + 1);
                self.push_indent(indent + 1);
                self.out.push_str(&format!(
                    "list_ensure({}.as.list, {}.as.list->len + 1);\n",
                    target, target
                ));
                self.push_indent(indent + 1);
                self.out.push_str(&format!(
                    "{}.as.list->items[{}.as.list->len++] = value_clone({});\n",
                    target, target, val_tmp
                ));
                self.push_indent(indent + 1);
                self.out
                    .push_str(&format!("value_free({});\n", val_tmp));
                self.push_indent(indent);
                self.out.push_str("}\n");
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", list_tmp));
            }
            Expr::DictComp { target: for_target, iter, key, value, cond } => {
                self.push_indent(indent);
                self.out
                    .push_str(&format!("Value {} = value_dict_literal(0, NULL, NULL);\n", target));
                let iter_tmp = self.new_temp();
                self.emit_expr(iter, &iter_tmp, indent);
                let list_tmp = self.new_temp();
                self.push_indent(indent);
                self.out.push_str(&format!(
                    "Value {} = value_iterable_to_list({});\n",
                    list_tmp, iter_tmp
                ));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", iter_tmp));
                let idx = self.new_sym("i");
                self.push_indent(indent);
                self.out.push_str(&format!(
                    "for (size_t {idx}=0; {idx} < {list}.as.list->len; {idx}++) {{\n",
                    idx = idx,
                    list = list_tmp
                ));
                let item_tmp = self.new_temp();
                self.push_indent(indent + 1);
                self.out.push_str(&format!(
                    "Value {} = value_clone({}.as.list->items[{}]);\n",
                    item_tmp, list_tmp, idx
                ));
                self.emit_for_target_bind(for_target, &item_tmp, indent + 1);
                self.push_indent(indent + 1);
                self.out
                    .push_str(&format!("value_free({});\n", item_tmp));
                if let Some(c) = cond {
                    let cond_tmp = self.new_temp();
                    self.emit_expr(c, &cond_tmp, indent + 1);
                    self.push_indent(indent + 1);
                    self.out.push_str(&format!(
                        "if (!value_truthy({})) {{ value_free({}); continue; }}\n",
                        cond_tmp, cond_tmp
                    ));
                    self.push_indent(indent + 1);
                    self.out
                        .push_str(&format!("value_free({});\n", cond_tmp));
                }
                let key_tmp = self.new_temp();
                let val_tmp = self.new_temp();
                self.emit_expr(key, &key_tmp, indent + 1);
                self.emit_expr(value, &val_tmp, indent + 1);
                self.push_indent(indent + 1);
                self.out.push_str(&format!(
                    "dict_set({}.as.dict, {}, {});\n",
                    target, key_tmp, val_tmp
                ));
                self.push_indent(indent + 1);
                self.out
                    .push_str(&format!("value_free({});\n", key_tmp));
                self.push_indent(indent + 1);
                self.out
                    .push_str(&format!("value_free({});\n", val_tmp));
                self.push_indent(indent);
                self.out.push_str("}\n");
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", list_tmp));
            }
            Expr::Var(name) => {
                self.push_indent(indent);
                self.out
                    .push_str(&format!("Value {} = value_clone({});\n", target, self.cname(name)));
            }
            Expr::Unary(op, expr) => {
                let tmp = self.new_temp();
                self.emit_expr(expr, &tmp, indent);
                match op {
                    UnOp::Neg => {
                        self.push_indent(indent);
                        self.out
                            .push_str(&format!("Value {} = value_neg({});\n", target, tmp));
                    }
                }
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", tmp));
            }
            Expr::Binary(left, op, right) => {
                let ltmp = self.new_temp();
                let rtmp = self.new_temp();
                self.emit_expr(left, &ltmp, indent);
                self.emit_expr(right, &rtmp, indent);
                let func = match op {
                    BinOp::Add => "value_add",
                    BinOp::Sub => "value_sub",
                    BinOp::Mul => "value_mul",
                    BinOp::Div => "value_div",
                };
                self.push_indent(indent);
                self.out
                    .push_str(&format!("Value {} = {}({}, {});\n", target, func, ltmp, rtmp));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", ltmp));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", rtmp));
            }
            Expr::Compare(left, op, right) => {
                let ltmp = self.new_temp();
                let rtmp = self.new_temp();
                self.emit_expr(left, &ltmp, indent);
                self.emit_expr(right, &rtmp, indent);
                let code = match op {
                    CmpOp::Eq => 0,
                    CmpOp::Ne => 1,
                    CmpOp::Lt => 2,
                    CmpOp::Le => 3,
                    CmpOp::Gt => 4,
                    CmpOp::Ge => 5,
                };
                self.push_indent(indent);
                self.out.push_str(&format!(
                    "Value {} = value_cmp({}, {}, {});\n",
                    target, ltmp, rtmp, code
                ));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", ltmp));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", rtmp));
            }
            Expr::Index(object, index) => {
                let obj_tmp = self.new_temp();
                let idx_tmp = self.new_temp();
                self.emit_expr(object, &obj_tmp, indent);
                self.emit_expr(index, &idx_tmp, indent);
                self.push_indent(indent);
                self.out.push_str(&format!(
                    "Value {} = value_index({}, {});\n",
                    target, obj_tmp, idx_tmp
                ));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", obj_tmp));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", idx_tmp));
            }
            Expr::Slice { value, start, end } => {
                let obj_tmp = self.new_temp();
                self.emit_expr(value, &obj_tmp, indent);
                let start_tmp = self.new_temp();
                if let Some(st) = start {
                    self.emit_expr(st, &start_tmp, indent);
                } else {
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("Value {} = value_int(0);\n", start_tmp));
                }
                let end_tmp = self.new_temp();
                if let Some(en) = end {
                    self.emit_expr(en, &end_tmp, indent);
                } else {
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("Value {} = value_int(0);\n", end_tmp));
                }
                let has_start = self.new_sym("has_start");
                let has_end = self.new_sym("has_end");
                self.push_indent(indent);
                self.out.push_str(&format!(
                    "int {} = {};\n",
                    has_start,
                    if start.is_some() { 1 } else { 0 }
                ));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("int {} = {};\n", has_end, if end.is_some() { 1 } else { 0 }));
                self.push_indent(indent);
                self.out.push_str(&format!(
                    "Value {} = value_slice({}, {} ? &{} : NULL, {}, {} ? &{} : NULL, {});\n",
                    target, obj_tmp, has_start, start_tmp, has_start, has_end, end_tmp, has_end
                ));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", obj_tmp));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", start_tmp));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", end_tmp));
            }
            Expr::MethodCall { object, method, arg } => {
                let obj_tmp = self.new_temp();
                self.emit_expr(object, &obj_tmp, indent);
                let mut args_vec = Vec::new();
                if let Some(a) = arg {
                    let tmpa = self.new_temp();
                    self.emit_expr(a, &tmpa, indent);
                    args_vec.push(tmpa);
                }
                let argc = args_vec.len();
                if argc == 0 {
                    self.push_indent(indent);
                    self.out.push_str(&format!(
                        "Value {} = value_method_call({}, \"{}\", 0, NULL);\n",
                        target, obj_tmp, method
                    ));
                } else {
                    let args_name = self.new_sym("args");
                    self.push_indent(indent);
                    self.out.push_str(&format!("Value {}[1];\n", args_name));
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("{}[0] = {};\n", args_name, args_vec[0]));
                    self.push_indent(indent);
                    self.out.push_str(&format!(
                        "Value {} = value_method_call({}, \"{}\", {}, {});\n",
                        target, obj_tmp, method, argc, args_name
                    ));
                }
                self.push_indent(indent);
                self.out.push_str(&format!("value_free({});\n", obj_tmp));
                for a in args_vec {
                    self.push_indent(indent);
                    self.out.push_str(&format!("value_free({});\n", a));
                }
            }
            Expr::Attr { object, attr } => {
                let obj_tmp = self.new_temp();
                self.emit_expr(object, &obj_tmp, indent);
                self.push_indent(indent);
                self.out
                    .push_str(&format!("Value {} = value_get_attr({}, \"{}\");\n", target, obj_tmp, attr));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", obj_tmp));
            }
            Expr::Call { callee, args } => {
                let mut tmp_args = Vec::new();
                for arg in args {
                    let t = self.new_temp();
                    self.emit_expr(arg, &t, indent);
                    tmp_args.push(t);
                }
                if let Expr::Var(name) = &**callee {
                    match name.as_str() {
                        "input" => {
                            let prompt = tmp_args.get(0);
                            let prompt_arg = match prompt {
                                Some(p) => p.as_str(),
                                None => {
                                    self.push_indent(indent);
                                    self.out.push_str("Value tmp_prompt = value_str(\"\");\n");
                                    tmp_args.push("tmp_prompt".to_string());
                                    "tmp_prompt"
                                }
                            };
                            self.push_indent(indent);
                            self.out.push_str(&format!(
                                "Value {} = builtin_input({});\n",
                                target, prompt_arg
                            ));
                            for t in tmp_args {
                                self.push_indent(indent);
                                self.out
                                    .push_str(&format!("value_free({});\n", t));
                            }
                            return;
                        }
                        "int" => {
                            self.push_indent(indent);
                            self.out
                                .push_str(&format!("Value {} = builtin_int({});\n", target, tmp_args[0]));
                            for t in tmp_args {
                                self.push_indent(indent);
                                self.out
                                    .push_str(&format!("value_free({});\n", t));
                            }
                            return;
                        }
                        "float" => {
                            self.push_indent(indent);
                            self.out.push_str(&format!(
                                "Value {} = builtin_float({});\n",
                                target, tmp_args[0]
                            ));
                            for t in tmp_args {
                                self.push_indent(indent);
                                self.out
                                    .push_str(&format!("value_free({});\n", t));
                            }
                            return;
                        }
                        "str" => {
                            self.push_indent(indent);
                            self.out
                                .push_str(&format!("Value {} = builtin_str({});\n", target, tmp_args[0]));
                            for t in tmp_args {
                                self.push_indent(indent);
                                self.out
                                    .push_str(&format!("value_free({});\n", t));
                            }
                            return;
                        }
                        "open" => {
                            if tmp_args.len() < 1 || tmp_args.len() > 2 {
                                self.push_indent(indent);
                                self.out.push_str(&format!(
                                    "Value {} = value_str(\"<invalid open args>\");\n",
                                    target
                                ));
                            } else {
                                let mode_arg = if tmp_args.len() == 2 { tmp_args[1].as_str() } else { "value_str(\"r\")" };
                                self.push_indent(indent);
                                self.out.push_str(&format!(
                                    "Value {} = builtin_open({}, {});\n",
                                    target, tmp_args[0], mode_arg
                                ));
                            }
                            for t in tmp_args {
                                self.push_indent(indent);
                                self.out
                                    .push_str(&format!("value_free({});\n", t));
                            }
                            return;
                        }
                        "sorted" => {
                            if tmp_args.is_empty() {
                                self.push_indent(indent);
                                self.out.push_str(&format!(
                                    "Value {} = value_list_literal(0, NULL);\n",
                                    target
                                ));
                            } else {
                                let has_key = tmp_args.len() > 1;
                                let key_arg = if has_key { tmp_args[1].clone() } else { "value_int(0)".to_string() };
                                self.push_indent(indent);
                                self.out.push_str(&format!(
                                    "Value {} = builtin_sorted({}, {}, {});\n",
                                    target, tmp_args[0], key_arg, if has_key { 1 } else { 0 }
                                ));
                            }
                            for t in tmp_args {
                                self.push_indent(indent);
                                self.out
                                    .push_str(&format!("value_free({});\n", t));
                            }
                            return;
                        }
                        "range" => {
                            let args_arr = self.new_sym("args");
                            self.push_indent(indent);
                            if tmp_args.is_empty() {
                                self.out
                                    .push_str(&format!("Value {} = builtin_range(0, NULL);\n", target));
                            } else {
                                self.out.push_str(&format!("Value {}[] = {{", args_arr));
                                for (idx, t) in tmp_args.iter().enumerate() {
                                    if idx > 0 {
                                        self.out.push_str(", ");
                                    }
                                    self.out.push_str(t);
                                }
                                self.out.push_str("};\n");
                                self.push_indent(indent);
                                self.out.push_str(&format!(
                                    "Value {} = builtin_range({}, {});\n",
                                    target,
                                    tmp_args.len(),
                                    args_arr
                                ));
                            }
                            for t in tmp_args {
                                self.push_indent(indent);
                                self.out
                                    .push_str(&format!("value_free({});\n", t));
                            }
                            return;
                        }
                        "enumerate" => {
                            if let Some(first) = tmp_args.get(0) {
                                self.push_indent(indent);
                                self.out.push_str(&format!(
                                    "Value {} = builtin_enumerate({});\n",
                                    target, first
                                ));
                            } else {
                                let empty = self.new_sym("empty_list");
                                self.push_indent(indent);
                                self.out
                                    .push_str(&format!("Value {} = value_list_literal(0, NULL);\n", empty));
                                self.push_indent(indent);
                                self.out.push_str(&format!(
                                    "Value {} = builtin_enumerate({});\n",
                                    target, empty
                                ));
                                self.push_indent(indent);
                                self.out
                                    .push_str(&format!("value_free({});\n", empty));
                            }
                            for t in tmp_args {
                                self.push_indent(indent);
                                self.out
                                    .push_str(&format!("value_free({});\n", t));
                            }
                            return;
                        }
                        _ => {}
                    }
                }
                let callee_tmp = self.new_temp();
                self.emit_expr(callee, &callee_tmp, indent);
                let args_arr = self.new_sym("args");
                self.push_indent(indent);
                self.out.push_str(&format!("Value {}[] = {{", args_arr));
                for (idx, t) in tmp_args.iter().enumerate() {
                    if idx > 0 {
                        self.out.push_str(", ");
                    }
                    self.out.push_str(t);
                }
                self.out.push_str("};\n");
                self.push_indent(indent);
                self.out.push_str(&format!(
                    "Value {} = value_call({}, {}, {});\n",
                    target,
                    callee_tmp,
                    tmp_args.len(),
                    args_arr
                ));
                self.push_indent(indent);
                self.out
                    .push_str(&format!("value_free({});\n", callee_tmp));
                for t in tmp_args {
                    self.push_indent(indent);
                    self.out
                        .push_str(&format!("value_free({});\n", t));
                }
            }
        }
    }
}

fn escape_c_string(value: &str) -> String {
    let mut escaped = String::new();
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\t' => escaped.push_str("\\t"),
            other => escaped.push(other),
        }
    }
    escaped
}

fn compile_c_to_exe(c_source: &str, output: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let tmp_name = format!(
        "pycomp_{}_{}.c",
        process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_millis()
    );
    let tmp_path = env::temp_dir().join(tmp_name);
    fs::write(&tmp_path, c_source)?;

    let status = Command::new("cc")
        .arg(&tmp_path)
        .arg("-o")
        .arg(output)
        .status();

    fs::remove_file(&tmp_path).ok();

    let status = status?;
    if !status.success() {
        return Err("cc failed to produce an executable".into());
    }

    Ok(())
}
