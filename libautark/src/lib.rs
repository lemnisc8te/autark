#[forbid(
    unused_unsafe,
    clippy::fallible_impl_from,
    clippy::used_underscore_binding,
    clippy::used_underscore_items,
    clippy::undocumented_unsafe_blocks
)]
// #[deny(
//     unreachable_pub,
//     unused_qualifications,
//     clippy::pedantic,
//     clippy::cargo,
//     clippy::nursery,
//     clippy::perf,
//     clippy::correctness,
//     clippy::suspicious,
//     clippy::complexity,
//     clippy::style,
//     clippy::branches_sharing_code,
//     clippy::use_self,
//     clippy::redundant_allocation,
//     clippy::deref_by_slicing,
//     clippy::cloned_instead_of_copied,
//     unused_allocation,
//     clippy::ptr_arg,
//     clippy::needless_pass_by_ref_mut,
//     clippy::needless_pass_by_value,
//     clippy::min_ident_chars
// )]
// #[warn(
//     clippy::panic,
//     clippy::unwrap_in_result,
//     clippy::large_stack_frames,
//     clippy::dbg_macro
// )]
// #[allow(
//     clippy::default_trait_access,
//     clippy::type_complexity,
//     clippy::missing_panics_doc,
//     unstable_name_collisions
// )]
pub mod engine;
pub mod model;

// use assert_no_alloc::*;

// #[cfg(debug_assertions)] // required when disable_release is set (default)
// #[global_allocator]
// static A: AllocDisabler = AllocDisabler;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_works() {
        todo!()
    }
}

// I think this is the correct diagram?
// ┌──┐                   ┌──────┐          ┌─────┐      ┌───────┐
// │UI│                   │Engine│          │Audio│      │Garbage│
// └┬─┘                   └──┬───┘          └──┬──┘      └───┬───┘
//  │                        │                 │             │
//  │ tk.mpsc<EngineCommand> │                 │             │
//  │───────────────────────>│                 │             │
//  │                        │                 │             │
//  │tk.oneshot<Box<dyn Any>>│                 │             │
//  │<───────────────────────│                 │             │
//  │                        │                 │             │
//  │                        │rtrb<GraphUpdate>│             │
//  │                        │────────────────>│             │
//  │                        │                 │             │
//  │                        │                 │rtrb<Garbage>│
//  │                        │                 │────────────>│
//  │                        │                 │             │
//  │             rtrb<UICommand>              │             │
//  │<─────────────────────────────────────────│             │
// ┌┴─┐                   ┌──┴───┐          ┌──┴──┐      ┌───┴───┐
// │UI│                   │Engine│          │Audio│      │Garbage│
// └──┘                   └──────┘          └─────┘      └───────┘
