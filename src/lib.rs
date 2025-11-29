use std::env;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{self, Command};
use std::time::{SystemTime, UNIX_EPOCH};

pub mod interpreter;
pub mod runtime;

pub fn run_cli() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!(
            "Usage:\n  {} <input.ty>                      # interpret\n  {} --build <input.ty> --out <exe>     # compile to binary",
            args[0], args[0]
        );
        process::exit(1);
    }

    let mut interpret_mode = true;
    let mut input_path: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--build" => {
                interpret_mode = false;
                i += 1;
                if i < args.len() {
                    input_path = Some(PathBuf::from(&args[i]));
                }
            }
            "--out" => {
                i += 1;
                if i < args.len() {
                    output_path = Some(PathBuf::from(&args[i]));
                }
            }
            other => {
                input_path = Some(PathBuf::from(other));
            }
        }
        i += 1;
    }

    let input_path = input_path.ok_or("Missing input path")?;
    if !input_path.exists() {
        return Err(format!("Input file not found: {}", input_path.display()).into());
    }

    let source = fs::read_to_string(&input_path)?;
    let program = parse_program(&source)?;
    if interpret_mode {
        let mut interp = interpreter::Interpreter::new();
        interp.run(&program)?;
        return Ok(());
    }

    let output_path = output_path.ok_or("Missing --out for build")?;
    compile_rust_runner(&source, &output_path)?;
    println!(
        "Compiled {} -> {}",
        input_path.display(),
        output_path.display()
    );
    Ok(())
}

#[derive(Debug, Clone)]
pub struct Program {
    stmts: Vec<Stmt>,
}

#[derive(Debug, Clone)]
enum Stmt {
    Function {
        name: String,
        params: Vec<String>,
        body: Vec<Stmt>,
    },
    Class {
        name: String,
        methods: Vec<Stmt>,
    },
    Assign {
        target: AssignTarget,
        expr: Expr,
        op: AssignOp,
    },
    Print {
        args: Vec<Expr>,
    },
    ExprStmt {
        expr: Expr,
    },
    Try {
        body: Vec<Stmt>,
        handlers: Vec<ExceptHandler>,
        finally_body: Option<Vec<Stmt>>,
    },
    Raise {
        expr: Expr,
    },
    If {
        branches: Vec<(Expr, Vec<Stmt>)>,
        else_branch: Option<Vec<Stmt>>,
    },
    While {
        cond: Expr,
        body: Vec<Stmt>,
    },
    For {
        target: ForTarget,
        iter: Expr,
        body: Vec<Stmt>,
    },
    With {
        target: String,
        expr: Expr,
        body: Vec<Stmt>,
    },
    Return {
        expr: Option<Expr>,
    },
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

#[derive(Debug, Clone, Copy, PartialEq)]
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
    Lambda {
        params: Vec<String>,
        body: Box<Expr>,
    },
    Unary(UnOp, Box<Expr>),
    Binary(Box<Expr>, BinOp, Box<Expr>),
    Compare(Box<Expr>, CmpOp, Box<Expr>),
    Call {
        callee: Box<Expr>,
        args: Vec<Expr>,
    },
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
    Not,
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
pub struct ParseError {
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

fn compile_rust_runner(source: &str, output_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let crate_root = env::current_dir()?.canonicalize()?;
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let runner_dir = crate_root
        .join("target")
        .join(format!("tyrion_build_{stamp}"));
    fs::create_dir_all(runner_dir.join("src"))?;

    let runner_cargo = format!(
        "[package]\nname = \"tyrion_runner\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dependencies]\ntyrion = {{ path = \"{}\" }}\n",
        crate_root.display()
    );
    fs::write(runner_dir.join("Cargo.toml"), runner_cargo)?;

    let source_literal = format!("{source:?}");
    let runner_main = format!(
        "fn main() {{\n    const SRC: &str = {source_literal};\n    let program = tyrion::parse_program(SRC).expect(\"parse failed\");\n    let mut interp = tyrion::interpreter::Interpreter::new();\n    if let Err(e) = interp.run(&program) {{\n        eprintln!(\"{{}}\", e);\n        std::process::exit(1);\n    }}\n}}\n"
    );
    fs::write(runner_dir.join("src").join("main.rs"), runner_main)?;

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
        .join("tyrion_runner");
    if !built.exists() {
        return Err("built binary not found".into());
    }
    fs::copy(&built, output_path)?;
    Ok(())
}

#[derive(Debug)]
struct LineInfo {
    line_no: usize,
    indent: usize,
    level: usize,
    content: String,
    raw: String,
    opens_brace: bool,
}

fn collect_lines(source: &str) -> Vec<LineInfo> {
    let mut lines = Vec::new();
    let mut brace_depth = 0usize;
    for (idx, raw_line) in source.lines().enumerate() {
        let line_no = idx + 1;
        let stripped = strip_comment(raw_line);
        let trimmed_end = stripped.trim_end();
        let mut content = trimmed_end.trim();
        while content.starts_with('}') {
            if brace_depth > 0 {
                brace_depth -= 1;
            }
            content = content[1..].trim_start();
        }
        if content.is_empty() {
            continue;
        }
        let without_indent = trimmed_end.trim_start_matches(|c: char| c == ' ' || c == '\t');
        let indent = trimmed_end.len() - without_indent.len();
        let mut opens_brace = false;
        if content.ends_with('{') {
            opens_brace = true;
            content = content[..content.rfind('{').unwrap()].trim_end();
        }
        if content.is_empty() {
            if opens_brace {
                brace_depth += 1;
            }
            continue;
        }
        let level = brace_depth * 1000 + indent;
        lines.push(LineInfo {
            line_no,
            indent,
            level,
            content: content.to_string(),
            raw: raw_line.to_string(),
            opens_brace,
        });
        if opens_brace {
            brace_depth += 1;
        }
    }
    lines
}

fn strip_comment(line: &str) -> String {
    let mut in_str = false;
    let mut escaped = false;
    let mut result = String::new();
    for ch in line.chars() {
        if escaped {
            result.push(ch);
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            result.push(ch);
            continue;
        }
        if ch == '"' {
            in_str = !in_str;
            result.push(ch);
            continue;
        }
        if ch == '#' && !in_str {
            break;
        }
        result.push(ch);
    }
    result
}

pub fn parse_program(source: &str) -> Result<Program, ParseError> {
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

fn parse_block(
    lines: &[LineInfo],
    start: usize,
    level: usize,
) -> Result<(Vec<Stmt>, usize), ParseError> {
    let mut stmts = Vec::new();
    let mut idx = start;
    while idx < lines.len() {
        let line = &lines[idx];
        if line.level < level {
            break;
        }
        if line.level > level {
            return Err(parse_error(
                line.line_no,
                line.indent + 1,
                "Unexpected indent",
                &line.raw,
            ));
        }
        let (stmt, next_idx) = parse_statement(lines, idx, level)?;
        stmts.push(stmt);
        idx = next_idx;
    }
    Ok((stmts, idx))
}

fn parse_statement(
    lines: &[LineInfo],
    idx: usize,
    level: usize,
) -> Result<(Stmt, usize), ParseError> {
    let line = &lines[idx];
    let content = line.content.as_str();
    let col_offset = line.indent + 1;
    if let Some((body_src, cond_src, is_unless)) = find_postfix_guard(content) {
        let body_stmt = parse_guard_body(&body_src, line)?;
        let cond_offset = line.raw.find(&cond_src).unwrap_or(line.indent) + 1;
        let (mut cond_expr, _) =
            parse_expression(&cond_src, 0, line.line_no, cond_offset, &line.raw)?;
        if is_unless {
            cond_expr = Expr::Unary(UnOp::Not, Box::new(cond_expr));
        }
        let stmt = Stmt::If {
            branches: vec![(cond_expr, vec![body_stmt])],
            else_branch: None,
        };
        return Ok((stmt, idx + 1));
    }
    if content.starts_with("def ") {
        parse_def(lines, idx, level)
    } else if content.starts_with("return") {
        parse_return(lines, idx)
    } else if content.starts_with("class ") {
        parse_class(lines, idx, level)
    } else if content.starts_with("try") {
        parse_try(lines, idx, level)
    } else if content.starts_with("raise") {
        parse_raise(lines, idx)
    } else if content.starts_with("if ") || content.starts_with("if(") {
        parse_if(lines, idx, level)
    } else if content.starts_with("while ") || content.starts_with("while(") {
        parse_while(lines, idx, level)
    } else if content.starts_with("for ") {
        parse_for(lines, idx, level)
    } else if content.starts_with("with ") {
        parse_with(lines, idx, level)
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

fn find_postfix_guard(line: &str) -> Option<(String, String, bool)> {
    let bytes = line.as_bytes();
    let mut in_str = false;
    let mut found: Option<(usize, usize, bool)> = None;
    let mut i = 0;
    while i < bytes.len() {
        let ch = bytes[i];
        if in_str {
            if ch == b'\\' {
                i += 2;
                continue;
            } else if ch == b'"' {
                in_str = false;
            }
            i += 1;
            continue;
        }
        if ch == b'"' {
            in_str = true;
            i += 1;
            continue;
        }
        let is_if = line[i..].starts_with(" if ");
        let is_unless = line[i..].starts_with(" unless ");
        if is_if || is_unless {
            let kw_len = if is_if { 4 } else { 8 }; // includes spaces
            found = Some((i, kw_len, is_unless));
        }
        i += 1;
    }
    let (pos, kw_len, is_unless) = found?;
    let body = line[..pos].trim_end();
    let cond = line[(pos + kw_len)..].trim();
    if body.is_empty() || cond.is_empty() {
        return None;
    }
    Some((body.to_string(), cond.to_string(), is_unless))
}

fn parse_guard_body(body_src: &str, line: &LineInfo) -> Result<Stmt, ParseError> {
    let fake_raw = format!("{}{}", " ".repeat(line.indent), body_src);
    let col_offset = line.indent + 1;
    if body_src.starts_with("return") {
        let rest = body_src.strip_prefix("return").unwrap().trim();
        if rest.is_empty() {
            return Ok(Stmt::Return { expr: None });
        }
        let (expr, next) = parse_expression(
            rest,
            0,
            line.line_no,
            col_offset + body_src.find(rest).unwrap_or(0),
            &fake_raw,
        )?;
        let next = skip_ws(rest.as_bytes(), next);
        if next != rest.len() {
            return Err(parse_error(
                line.line_no,
                col_offset + next,
                "Unexpected trailing characters after return expression",
                &fake_raw,
            ));
        }
        return Ok(Stmt::Return { expr: Some(expr) });
    }
    if body_src.starts_with("raise") {
        let rest = body_src.strip_prefix("raise").unwrap().trim();
        if rest.is_empty() {
            return Err(parse_error(
                line.line_no,
                col_offset + "raise".len(),
                "Missing expression after raise",
                &fake_raw,
            ));
        }
        let (expr, next) = parse_expression(
            rest,
            0,
            line.line_no,
            col_offset + body_src.find(rest).unwrap_or(0),
            &fake_raw,
        )?;
        let next = skip_ws(rest.as_bytes(), next);
        if next != rest.len() {
            return Err(parse_error(
                line.line_no,
                col_offset + next,
                "Unexpected trailing characters after raise expression",
                &fake_raw,
            ));
        }
        return Ok(Stmt::Raise { expr });
    }
    if body_src.starts_with("print") {
        return parse_print(body_src, line.line_no, col_offset, &fake_raw);
    }
    if find_assign_op(body_src).is_some() {
        return parse_assign(body_src, line.line_no, col_offset, &fake_raw);
    }
    let (expr, next) = parse_expression(body_src, 0, line.line_no, col_offset, &fake_raw)?;
    let next = skip_ws(body_src.as_bytes(), next);
    if next != body_src.len() {
        return Err(parse_error(
            line.line_no,
            col_offset + next,
            "Unexpected trailing characters",
            &fake_raw,
        ));
    }
    Ok(Stmt::ExprStmt { expr })
}

fn parse_if(lines: &[LineInfo], idx: usize, level: usize) -> Result<(Stmt, usize), ParseError> {
    let line = &lines[idx];
    let content = line.content.as_str();
    if !content.starts_with("if") {
        return Err(parse_error(
            line.line_no,
            line.indent + 1,
            "Expected 'if'",
            &line.raw,
        ));
    }
    let mut cond_src = content[2..].trim();
    let mut has_colon = false;
    if cond_src.ends_with(':') {
        has_colon = true;
        cond_src = cond_src[..cond_src.len().saturating_sub(1)].trim_end();
    }
    if cond_src.is_empty() {
        return Err(parse_error(
            line.line_no,
            line.indent + 1,
            "Expected condition after if",
            &line.raw,
        ));
    }
    if !has_colon && !line.opens_brace {
        return Err(parse_error(
            line.line_no,
            line.indent + content.len(),
            "Expected ':' after if condition",
            &line.raw,
        ));
    }
    let cond_offset = line.raw.find(cond_src).unwrap_or(line.indent) + 1;
    let (cond, _) = parse_expression(cond_src, 0, line.line_no, cond_offset, &line.raw)?;
    if idx + 1 >= lines.len() || lines[idx + 1].level <= level {
        return Err(parse_error(
            line.line_no,
            line.indent + content.len(),
            "Missing indented block after if",
            &line.raw,
        ));
    }
    let body_level = lines[idx + 1].level;
    let (body, mut next_idx) = parse_block(lines, idx + 1, body_level)?;
    let mut branches = vec![(cond, body)];
    let mut else_branch = None;
    while next_idx < lines.len() && lines[next_idx].level == level {
        let nxt = &lines[next_idx];
        if nxt.content.starts_with("elif") {
            let mut cond_src = nxt.content[4..].trim();
            let mut has_colon = false;
            if cond_src.ends_with(':') {
                has_colon = true;
                cond_src = cond_src[..cond_src.len().saturating_sub(1)].trim_end();
            }
            if cond_src.is_empty() {
                return Err(parse_error(
                    nxt.line_no,
                    nxt.indent + 1,
                    "Expected condition after elif",
                    &nxt.raw,
                ));
            }
            if !has_colon && !nxt.opens_brace {
                return Err(parse_error(
                    nxt.line_no,
                    nxt.indent + nxt.content.len(),
                    "Expected ':' after elif condition",
                    &nxt.raw,
                ));
            }
            let cond_offset = nxt.raw.find(cond_src).unwrap_or(nxt.indent) + 1;
            let (elif_cond, _) = parse_expression(cond_src, 0, nxt.line_no, cond_offset, &nxt.raw)?;
            if next_idx + 1 >= lines.len() || lines[next_idx + 1].level <= level {
                return Err(parse_error(
                    nxt.line_no,
                    nxt.indent + nxt.content.len(),
                    "Missing indented block after elif",
                    &nxt.raw,
                ));
            }
            let body_level = lines[next_idx + 1].level;
            let (elif_body, n2) = parse_block(lines, next_idx + 1, body_level)?;
            branches.push((elif_cond, elif_body));
            next_idx = n2;
            continue;
        } else if nxt.content.starts_with("else") {
            let mut tail = nxt.content["else".len()..].trim();
            let mut has_colon = false;
            if tail.ends_with(':') {
                has_colon = true;
                tail = tail[..tail.len().saturating_sub(1)].trim_end();
            }
            if !tail.is_empty() {
                return Err(parse_error(
                    nxt.line_no,
                    nxt.indent + nxt.content.len(),
                    "Unexpected characters after ':'",
                    &nxt.raw,
                ));
            }
            if !has_colon && !nxt.opens_brace {
                return Err(parse_error(
                    nxt.line_no,
                    nxt.indent + nxt.content.len(),
                    "Expected ':' after else",
                    &nxt.raw,
                ));
            }
            if next_idx + 1 >= lines.len() || lines[next_idx + 1].level <= level {
                return Err(parse_error(
                    nxt.line_no,
                    nxt.indent + nxt.content.len(),
                    "Missing indented block after else",
                    &nxt.raw,
                ));
            }
            let body_level = lines[next_idx + 1].level;
            let (body, n2) = parse_block(lines, next_idx + 1, body_level)?;
            else_branch = Some(body);
            next_idx = n2;
            break;
        } else {
            break;
        }
    }
    Ok((
        Stmt::If {
            branches,
            else_branch,
        },
        next_idx,
    ))
}

fn parse_while(lines: &[LineInfo], idx: usize, level: usize) -> Result<(Stmt, usize), ParseError> {
    let line = &lines[idx];
    let content = line.content.as_str();
    let mut cond_src = content[5..].trim();
    let mut has_colon = false;
    if cond_src.ends_with(':') {
        has_colon = true;
        cond_src = cond_src[..cond_src.len().saturating_sub(1)].trim_end();
    }
    if cond_src.is_empty() {
        return Err(parse_error(
            line.line_no,
            line.indent + 1,
            "Expected condition after while",
            &line.raw,
        ));
    }
    if !has_colon && !line.opens_brace {
        return Err(parse_error(
            line.line_no,
            line.indent + content.len(),
            "Expected ':' after while condition",
            &line.raw,
        ));
    }
    let cond_offset = line.raw.find(cond_src).unwrap_or(line.indent) + 1;
    let (cond, _) = parse_expression(cond_src, 0, line.line_no, cond_offset, &line.raw)?;
    if idx + 1 >= lines.len() || lines[idx + 1].level <= level {
        return Err(parse_error(
            line.line_no,
            line.indent + content.len(),
            "Missing indented block after while",
            &line.raw,
        ));
    }
    let body_level = lines[idx + 1].level;
    let (body, next_idx) = parse_block(lines, idx + 1, body_level)?;
    Ok((Stmt::While { cond, body }, next_idx))
}

fn parse_for(lines: &[LineInfo], idx: usize, level: usize) -> Result<(Stmt, usize), ParseError> {
    let line = &lines[idx];
    let content = line.content.as_str();
    let mut head = content[3..].trim();
    let mut has_colon = false;
    if head.ends_with(':') {
        has_colon = true;
        head = head[..head.len().saturating_sub(1)].trim_end();
    }
    if !has_colon && !line.opens_brace {
        return Err(parse_error(
            line.line_no,
            line.indent + content.len(),
            "Expected ':' after for clause",
            &line.raw,
        ));
    }
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
    if idx + 1 >= lines.len() || lines[idx + 1].level <= level {
        return Err(parse_error(
            line.line_no,
            line.indent + content.len(),
            "Missing indented block after for",
            &line.raw,
        ));
    }
    let body_level = lines[idx + 1].level;
    let (body, next_idx) = parse_block(lines, idx + 1, body_level)?;
    Ok((
        Stmt::For {
            target,
            iter: iter_expr,
            body,
        },
        next_idx,
    ))
}

fn parse_def(lines: &[LineInfo], idx: usize, level: usize) -> Result<(Stmt, usize), ParseError> {
    let line = &lines[idx];
    let content = line.content.as_str();
    let mut head = content[3..].trim();
    let mut has_colon = false;
    if head.ends_with(':') {
        has_colon = true;
        head = head[..head.len().saturating_sub(1)].trim_end();
    }
    if !has_colon && !line.opens_brace {
        return Err(parse_error(
            line.line_no,
            line.indent + content.len(),
            "Expected ':' after function signature",
            &line.raw,
        ));
    }
    let open = head
        .find('(')
        .ok_or_else(|| parse_error(line.line_no, line.indent + 4, "Expected '('", &line.raw))?;
    let close = head.rfind(')').ok_or_else(|| {
        parse_error(
            line.line_no,
            line.indent + head.len(),
            "Missing ')'",
            &line.raw,
        )
    })?;
    let name = head[..open].trim();
    if !is_valid_ident(name) {
        return Err(parse_error(
            line.line_no,
            line.indent + 1,
            "Invalid function name",
            &line.raw,
        ));
    }
    let params_src = &head[(open + 1)..close];
    let mut params = Vec::new();
    if !params_src.trim().is_empty() {
        for p in params_src.split(',') {
            let p = p.trim();
            if !is_valid_ident(p) {
                return Err(parse_error(
                    line.line_no,
                    line.indent + 1,
                    "Invalid parameter",
                    &line.raw,
                ));
            }
            params.push(p.to_string());
        }
    }
    if idx + 1 >= lines.len() || lines[idx + 1].level <= level {
        return Err(parse_error(
            line.line_no,
            line.indent + content.len(),
            "Missing indented block after function",
            &line.raw,
        ));
    }
    let body_level = lines[idx + 1].level;
    let (body, next_idx) = parse_block(lines, idx + 1, body_level)?;
    Ok((
        Stmt::Function {
            name: name.to_string(),
            params,
            body,
        },
        next_idx,
    ))
}

fn parse_class(lines: &[LineInfo], idx: usize, level: usize) -> Result<(Stmt, usize), ParseError> {
    let line = &lines[idx];
    let content = line.content.as_str();
    let mut name_src = content[5..].trim();
    let mut has_colon = false;
    if name_src.ends_with(':') {
        has_colon = true;
        name_src = name_src[..name_src.len().saturating_sub(1)].trim_end();
    }
    let name = name_src;
    if !is_valid_ident(name) {
        return Err(parse_error(
            line.line_no,
            line.indent + 1,
            "Invalid class name",
            &line.raw,
        ));
    }
    if !has_colon && !line.opens_brace {
        return Err(parse_error(
            line.line_no,
            line.indent + content.len(),
            "Expected ':' after class name",
            &line.raw,
        ));
    }
    if idx + 1 >= lines.len() || lines[idx + 1].level <= level {
        return Err(parse_error(
            line.line_no,
            line.indent + content.len(),
            "Missing indented block after class",
            &line.raw,
        ));
    }
    let body_level = lines[idx + 1].level;
    let (body, next_idx) = parse_block(lines, idx + 1, body_level)?;
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
    Ok((
        Stmt::Class {
            name: name.to_string(),
            methods,
        },
        next_idx,
    ))
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

fn parse_try(lines: &[LineInfo], idx: usize, level: usize) -> Result<(Stmt, usize), ParseError> {
    let line = &lines[idx];
    let content = line.content.as_str();
    let mut tail = content["try".len()..].trim();
    let mut has_colon = false;
    if tail.ends_with(':') {
        has_colon = true;
        tail = tail[..tail.len().saturating_sub(1)].trim_end();
    }
    if !tail.is_empty() {
        return Err(parse_error(
            line.line_no,
            line.indent + content.len(),
            "Unexpected characters after try",
            &line.raw,
        ));
    }
    if !has_colon && !line.opens_brace {
        return Err(parse_error(
            line.line_no,
            line.indent + content.len(),
            "Expected ':' after try",
            &line.raw,
        ));
    }
    if idx + 1 >= lines.len() || lines[idx + 1].level <= level {
        return Err(parse_error(
            line.line_no,
            line.indent + content.len(),
            "Missing indented block after try",
            &line.raw,
        ));
    }
    let body_level = lines[idx + 1].level;
    let (body, mut next_idx) = parse_block(lines, idx + 1, body_level)?;
    let mut handlers = Vec::new();
    let mut finally_body = None;
    while next_idx < lines.len() && lines[next_idx].level == level {
        let nxt = &lines[next_idx];
        if nxt.content.starts_with("except") {
            let mut head = nxt.content["except".len()..].trim();
            let mut has_colon = false;
            if head.ends_with(':') {
                has_colon = true;
                head = head[..head.len().saturating_sub(1)].trim_end();
            }
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
            if !has_colon && !nxt.opens_brace {
                return Err(parse_error(
                    nxt.line_no,
                    nxt.indent + nxt.content.len(),
                    "Expected ':' after except",
                    &nxt.raw,
                ));
            }
            if next_idx + 1 >= lines.len() || lines[next_idx + 1].level <= level {
                return Err(parse_error(
                    nxt.line_no,
                    nxt.indent + nxt.content.len(),
                    "Missing indented block after except",
                    &nxt.raw,
                ));
            }
            let body_level = lines[next_idx + 1].level;
            let (handler_body, n2) = parse_block(lines, next_idx + 1, body_level)?;
            handlers.push(ExceptHandler {
                name,
                bind,
                body: handler_body,
            });
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
            let mut tail = nxt.content["finally".len()..].trim();
            let mut has_colon = false;
            if tail.ends_with(':') {
                has_colon = true;
                tail = tail[..tail.len().saturating_sub(1)].trim_end();
            }
            if !tail.is_empty() {
                return Err(parse_error(
                    nxt.line_no,
                    nxt.indent + nxt.content.len(),
                    "Unexpected characters after finally",
                    &nxt.raw,
                ));
            }
            if !has_colon && !nxt.opens_brace {
                return Err(parse_error(
                    nxt.line_no,
                    nxt.indent + nxt.content.len(),
                    "Expected ':' after finally",
                    &nxt.raw,
                ));
            }
            if next_idx + 1 >= lines.len() || lines[next_idx + 1].level <= level {
                return Err(parse_error(
                    nxt.line_no,
                    nxt.indent + nxt.content.len(),
                    "Missing indented block after finally",
                    &nxt.raw,
                ));
            }
            let body_level = lines[next_idx + 1].level;
            let (f_body, n2) = parse_block(lines, next_idx + 1, body_level)?;
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
    Ok((
        Stmt::Try {
            body,
            handlers,
            finally_body,
        },
        next_idx,
    ))
}

fn parse_with(lines: &[LineInfo], idx: usize, level: usize) -> Result<(Stmt, usize), ParseError> {
    let line = &lines[idx];
    let content = line.content.as_str();
    let mut head = content[4..].trim();
    let mut has_colon = false;
    if head.ends_with(':') {
        has_colon = true;
        head = head[..head.len().saturating_sub(1)].trim_end();
    }
    if !has_colon && !line.opens_brace {
        return Err(parse_error(
            line.line_no,
            line.indent + content.len(),
            "Expected ':' after with clause",
            &line.raw,
        ));
    }
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
    if idx + 1 >= lines.len() || lines[idx + 1].level <= level {
        return Err(parse_error(
            line.line_no,
            line.indent + content.len(),
            "Missing indented block after with",
            &line.raw,
        ));
    }
    let body_level = lines[idx + 1].level;
    let (body, next_idx) = parse_block(lines, idx + 1, body_level)?;
    Ok((
        Stmt::With {
            target: target_src.to_string(),
            expr,
            body,
        },
        next_idx,
    ))
}

fn parse_for_target(
    segment: &str,
    line_no: usize,
    column_offset: usize,
    line_src: &str,
) -> Result<ForTarget, ParseError> {
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

fn parse_print(
    line: &str,
    line_no: usize,
    column_offset: usize,
    line_src: &str,
) -> Result<Stmt, ParseError> {
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

fn parse_assign(
    line: &str,
    line_no: usize,
    column_offset: usize,
    line_src: &str,
) -> Result<Stmt, ParseError> {
    let (op_pos, op) = find_assign_op(line)
        .ok_or_else(|| parse_error(line_no, column_offset, "Expected '='", line_src))?;
    let op_len = if matches!(op, AssignOp::Assign) { 1 } else { 2 };
    let (left, right) = line.split_at(op_pos);
    let lhs_str = left.trim();
    if lhs_str.is_empty() {
        return Err(parse_error(
            line_no,
            column_offset,
            "Missing assignment target",
            line_src,
        ));
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
    let (expr, next) = parse_expression(
        expr_str,
        0,
        line_no,
        line_src.find(expr_str).unwrap_or(column_offset) + 1,
        line_src,
    )?;
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

fn parse_assign_target(
    segment: &str,
    line_no: usize,
    column_offset: usize,
    line_src: &str,
) -> Result<AssignTarget, ParseError> {
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
            Expr::Var(name) => Ok(AssignTarget::Subscript {
                object: Expr::Var(name),
                index: *idx,
            }),
            _ => Err(parse_error(
                line_no,
                lhs_offset,
                "Can only assign to a variable subscript",
                line_src,
            )),
        },
        Expr::Attr { object, attr } => match *object {
            Expr::Var(name) => Ok(AssignTarget::Attr {
                object: Expr::Var(name),
                attr,
            }),
            Expr::Attr { .. } => Ok(AssignTarget::Attr {
                object: *object,
                attr,
            }),
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

fn parse_args(
    segment: &str,
    line_no: usize,
    column_offset: usize,
    line_src: &str,
) -> Result<Vec<Expr>, ParseError> {
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
        return Err(parse_error(
            line_no,
            column_offset + i + 1,
            "Expected lambda",
            line_src,
        ));
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
                return Err(parse_error(
                    line_no,
                    column_offset + params_start + 1,
                    "Invalid lambda param",
                    line_src,
                ));
            }
            params.push(p.to_string());
        }
    }
    i += 1; // skip ':'
    let body_start = skip_ws(bytes, i);
    let (body_expr, next) =
        parse_expression(segment, body_start, line_no, column_offset, line_src)?;
    Ok((
        Expr::Lambda {
            params,
            body: Box::new(body_expr),
        },
        next,
    ))
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
    if i < bytes.len() && bytes[i] == b'!' {
        let (expr, next) = parse_primary(segment, i + 1, line_no, column_offset, line_src)?;
        return Ok((Expr::Unary(UnOp::Not, Box::new(expr)), next));
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
            let (new_expr, next) =
                parse_call(expr, segment, idx, line_no, column_offset, line_src)?;
            expr = new_expr;
            idx = skip_ws(bytes, next);
            continue;
        }
        if idx < bytes.len() && bytes[idx] == b'[' {
            let (new_expr, next) =
                parse_subscript(expr, segment, idx, line_no, column_offset, line_src)?;
            expr = new_expr;
            idx = skip_ws(bytes, next);
            continue;
        }
        if idx < bytes.len() && bytes[idx] == b'.' {
            let (new_expr, next) =
                parse_method_call(expr, segment, idx, line_no, column_offset, line_src)?;
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
        return Err(parse_error(
            line_no,
            column_offset + start + 1,
            "Missing ')'",
            line_src,
        ));
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
                return Err(parse_error(
                    line_no,
                    column_offset + idx + 1,
                    "Missing ')'",
                    line_src,
                ));
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
        return Err(parse_error(
            line_no,
            column_offset + i + 1,
            "Missing ']'",
            line_src,
        ));
    }
    if bytes[i] == b']' {
        return Ok((Expr::List(Vec::new()), i + 1));
    }

    let (first_expr, mut next) = parse_expression(segment, i, line_no, column_offset, line_src)?;
    next = skip_ws(bytes, next);
    // comprehension?
    let rem = segment[next..].trim_start();
    if rem.starts_with("for ") {
        let after_for = next + (segment[next..].len() - rem.len()) + "for ".len();
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
        let after_in = after_for + in_pos + " in ".len();
        let rest_after_in = &segment[after_in..];
        let (iter_src, cond_src) = if let Some(pos) = rest_after_in.find(" if ") {
            let (iter_part, cond_part) = rest_after_in.split_at(pos);
            (iter_part.trim_end(), Some(cond_part[" if ".len()..].trim()))
        } else {
            (rest_after_in.trim_end(), None)
        };
        let iter_src = iter_src.trim_end_matches(']').trim_end();
        let (iter_expr, _) =
            parse_expression(iter_src, 0, line_no, column_offset + after_in, line_src)?;
        let mut cond = None;
        if let Some(csrc) = cond_src {
            let csrc = csrc.trim_end_matches(']').trim_end();
            let (cond_expr, _) =
                parse_expression(csrc, 0, line_no, column_offset + after_in, line_src)?;
            cond = Some(Box::new(cond_expr));
        }
        let end_pos = segment.len();
        return Ok((
            Expr::ListComp {
                target,
                iter: Box::new(iter_expr),
                expr: Box::new(first_expr),
                cond,
            },
            end_pos,
        ));
    }

    // regular list literal
    let mut items = vec![first_expr];
    i = next;
    loop {
        if i >= bytes.len() {
            return Err(parse_error(
                line_no,
                column_offset + i + 1,
                "Missing ']'",
                line_src,
            ));
        }
        if bytes[i] == b']' {
            return Ok((Expr::List(items), i + 1));
        }
        if bytes[i] == b',' {
            i = skip_ws(bytes, i + 1);
            if i >= bytes.len() {
                return Err(parse_error(
                    line_no,
                    column_offset + i + 1,
                    "Missing ']'",
                    line_src,
                ));
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
        return Err(parse_error(
            line_no,
            column_offset + i + 1,
            "Missing '}'",
            line_src,
        ));
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
        let rem = segment[nxt..].trim_start();
        if rem.starts_with("for ") {
            let after_for = nxt + (segment[nxt..].len() - rem.len()) + "for ".len();
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
            let target =
                parse_for_target(target_src, line_no, column_offset + after_for, line_src)?;
            let after_in = after_for + in_pos + " in ".len();
            let rest_after_in = &segment[after_in..];
            let (iter_src, cond_src) = if let Some(pos) = rest_after_in.find(" if ") {
                let (iter_part, cond_part) = rest_after_in.split_at(pos);
                (iter_part.trim_end(), Some(cond_part[" if ".len()..].trim()))
            } else {
                (rest_after_in.trim_end(), None)
            };
            let iter_src = iter_src.trim_end_matches('}').trim_end();
            let (iter_expr, _) =
                parse_expression(iter_src, 0, line_no, column_offset + after_in, line_src)?;
            let mut cond = None;
            if let Some(csrc) = cond_src {
                let csrc = csrc.trim_end_matches('}').trim_end();
                let (cond_expr, _) =
                    parse_expression(csrc, 0, line_no, column_offset + after_in, line_src)?;
                cond = Some(Box::new(cond_expr));
            }
            let end_pos = segment.len();
            let (k, v) = entries.remove(0);
            return Ok((
                Expr::DictComp {
                    target,
                    iter: Box::new(iter_expr),
                    key: Box::new(k),
                    value: Box::new(v),
                    cond,
                },
                end_pos,
            ));
        }
        if nxt < bytes.len() && bytes[nxt] == b',' {
            i = nxt + 1;
            loop {
                i = skip_ws(bytes, i);
                if i >= bytes.len() {
                    return Err(parse_error(
                        line_no,
                        column_offset + i + 1,
                        "Missing '}'",
                        line_src,
                    ));
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
                let (v, mut n3) =
                    parse_expression(segment, n2 + 1, line_no, column_offset, line_src)?;
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
                return Err(parse_error(
                    line_no,
                    column_offset + i + 1,
                    "Missing '}'",
                    line_src,
                ));
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
    let (colon_pos, close_idx) =
        scan_subscript(segment, open_idx, line_no, column_offset, line_src)?;
    let content_start = skip_ws(bytes, open_idx + 1);
    if let Some(colon) = colon_pos {
        let mut start_expr = None;
        if content_start < colon {
            let (s, next) =
                parse_expression(segment, content_start, line_no, column_offset, line_src)?;
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
        let (index_expr, next) =
            parse_expression(segment, content_start, line_no, column_offset, line_src)?;
        let next = skip_ws(bytes, next);
        if next != close_idx {
            return Err(parse_error(
                line_no,
                column_offset + close_idx + 1,
                "Unexpected trailing content in index",
                line_src,
            ));
        }
        Ok((
            Expr::Index(Box::new(object), Box::new(index_expr)),
            close_idx + 1,
        ))
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
            return Err(parse_error(
                line_no,
                column_offset + i + 1,
                "Missing ')'",
                line_src,
            ));
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
            return Err(parse_error(
                line_no,
                column_offset + i + 1,
                "Missing ')'",
                line_src,
            ));
        }
        if bytes[i] == b')' {
            return Ok((
                Expr::Call {
                    callee: Box::new(callee),
                    args,
                },
                i + 1,
            ));
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
                    return Ok((
                        Expr::Call {
                            callee: Box::new(callee),
                            args,
                        },
                        i + 1,
                    ));
                } else if i >= bytes.len() {
                    return Err(parse_error(
                        line_no,
                        column_offset + i + 1,
                        "Missing ')'",
                        line_src,
                    ));
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
            return Ok((
                Expr::Call {
                    callee: Box::new(callee),
                    args,
                },
                i + 1,
            ));
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
