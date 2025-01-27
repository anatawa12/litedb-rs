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
        $struct_vis:vis struct $struct_name: ident $(< $($generics: ident $(: $constraint0: path )? ),+ >)? {
            $(
            $(#[$field_meta:meta])*
            $field_vis:vis $field_name:ident : $field_ty:ty
            ),*
            $(,)?
        }
    ) => {
        $(#[$meta])*
        $struct_vis struct $struct_name $(< $($generics $(: $constraint0 )*),+ >)? {
            $($(#[$field_meta])* $field_vis $field_name: $field_ty,)*
        }

        const _: () = {
            struct Destructed $(< $($generics $(: $constraint0 )*),*+>)? {
                $($(#[$field_meta])* $field_vis $field_name: $field_ty,)*
            }

            impl  $(< $($generics $(: $constraint0 )*),* >)? $struct_name $(< $($generics),+ >)? {
                /// Converts this to destructed struct which does not implement drop

                fn into_destruct(self) -> Destructed $(< $($generics),*>)?  {
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
