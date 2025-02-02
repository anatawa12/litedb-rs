#[allow(unused)]
macro_rules! document {
    {$($k:expr => $v:expr),* $(,)?} => {{
        #[allow(unused_mut)]
        let mut doc = $crate::bson::Document::new();
        $(doc.insert($k.into(), $v);)*
        doc
    }}
}

#[allow(unused)]
macro_rules! array {
    [$($element:expr),* $(,)?] => {{
        #[allow(unused_mut)]
        let mut arr = $crate::bson::Array::new();
        $(arr.push($element);)*
        arr
    }};
}

#[allow(unused)]
macro_rules! date {
    [
        $year:tt-$month:tt-$day:tt
        $hour:tt:$minute:tt:$second:tt
    ] => {
        const {
            match $crate::bson::DateTime::parse_rfc3339(::core::concat!(core::stringify!($year), '-', core::stringify!($month), '-', core::stringify!($day), 'T', core::stringify!($hour), ':', core::stringify!($minute), ':', core::stringify!($second))) {
                Some(v) => v,
                None => {
                    ::core::panic!(::core::concat!("bad date:", core::stringify!($year), '-', core::stringify!($month), '-', core::stringify!($day), 'T', core::stringify!($hour), ':', core::stringify!($minute), ':', core::stringify!($second)))
                }
            }
        }
    };
}

#[allow(unused)]
macro_rules! decimal {
    ($value: literal) => {
        const {
            match $crate::bson::Decimal128::parse(::core::stringify!($value)) {
                Some(v) => v,
                None => {
                    ::core::panic!(::core::concat!("bad decimal: ", ::core::stringify!($value)));
                }
            }
        }
    };
}
