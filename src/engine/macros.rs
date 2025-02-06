macro_rules! into_ok {
    ($expr: expr) => {
        match $expr {
            ::std::result::Result::Ok(ok) => ok,
            ::std::result::Result::Err(e) => match e {},
        }
    };
}

macro_rules! into_non_drop {
    (
        $(#[$meta:meta])*
        $struct_vis:vis struct $struct_name: ident $(< $($lifetime: lifetime),* $($generics: ident $(: $constraint0: path )? ),* >)?
            $(where $where_type: ty : $bound: path)*
        {
            $(
            $(#[$field_meta:meta])*
            $field_vis:vis $field_name:ident : $field_ty:ty
            ),*
            $(,)?
        }
    ) => {
        $(#[$meta])*
        $struct_vis struct $struct_name $(< $($lifetime),* $($generics $(: $constraint0 )*),* >)? {
            $($(#[$field_meta])* $field_vis $field_name: $field_ty,)*
        }

        const _: () = {
            struct Destructed $(< $($lifetime),* $($generics $(: $constraint0 )*),* >)? {
                $($(#[$field_meta])* $field_vis $field_name: $field_ty,)*
            }

            impl  $(< $($lifetime),* $($generics $(: $constraint0 )*),* >)? $struct_name $(< $($lifetime),* $($generics),* >)? {
                /// Converts this to destructed struct which does not implement drop

                fn into_destruct(self) -> Destructed $(< $($lifetime),* $($generics),*>)?  {
                    let mut manually_drop = ::core::mem::ManuallyDrop::new(self);

                    unsafe {
                        Destructed {
                            $($field_name: ::core::ptr::read(&mut manually_drop.$field_name), )*
                        }
                    }
                }
            }
        };
    };
}

macro_rules! extend_lifetime {
    ($($path: ident)::+) => {
        unsafe impl<'a> $crate::engine::utils::ExtendLifetime<'a> for $($path)::+ <'_> {
            type Extended = $($path)::+<'a>;
            unsafe fn extend_lifetime(self) -> Self::Extended {
                unsafe { std::mem::transmute::<Self, Self::Extended>(self) }
            }
        }
    };
}

macro_rules! debug_log {
    ($category: ident: $($tt:tt)*) => {
        if cfg!(feature = "debug-logs") {
            std::println!("[{}||{}] {}", core::stringify!($category), core::module_path!(), core::format_args!($($tt)*));
        }
    }
}
