macro_rules! into_ok {
    ($expr: expr) => {
        match $expr {
            ::std::result::Result::Ok(ok) => ok,
            ::std::result::Result::Err(e) => match e {},
        }
    };
}
