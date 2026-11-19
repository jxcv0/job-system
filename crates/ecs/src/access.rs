use std::marker::PhantomData;

use crate::{
    World,
    components::{ComponentId, IdRegister},
};

use bit_set::BitSet;

#[derive(Clone, Default)]
pub(crate) struct AccessSet {
    reads: BitSet,
    writes: BitSet,
}

impl AccessSet {
    pub(crate) fn new() -> Self {
        Default::default()
    }

    pub(crate) fn reads(&mut self, id: ComponentId) {
        let idx = id.index();
        assert!(
            !self.writes.contains(idx),
            "a system can't read {id:?}, it already writes it"
        );
        self.reads.insert(idx);
    }

    pub(crate) fn writes(&mut self, id: ComponentId) {
        let idx = id.index();
        assert!(
            !self.reads.contains(idx) && !self.writes.contains(idx),
            "a system can't write {id:?} twice or read and write it"
        );
        self.writes.insert(idx);
    }

    pub(crate) fn conflicts(&self, other: &AccessSet) -> bool {
        !self.writes.is_disjoint(&other.reads)
            || !other.writes.is_disjoint(&self.reads)
            || !self.writes.is_disjoint(&other.writes)
    }
}

pub(crate) trait Access {
    /// What the system fn closure recieves
    type Item<'world>;

    /// Created once at buil() and kept for the lifetime of the schedule, the system gets the same
    /// &mut T with each call
    type State: Send + Sync + 'static;

    // declare access and create state
    fn init(ids: &mut IdRegister, set: &mut AccessSet) -> Self::State;

    /// Access the colums in component storage
    unsafe fn fetch<'world>(state: &'world Self::State, world: &'world World)
    -> Self::Item<'world>;
}

pub(crate) struct Read<T>(PhantomData<T>);
pub(crate) struct Write<T>(PhantomData<T>);

impl Access for () {
    type Item<'world> = ();
    type State = ();

    fn init(_ids: &mut IdRegister, _set: &mut AccessSet) -> Self::State {}

    unsafe fn fetch<'world>(
        _state: &'world Self::State,
        _world: &'world World,
    ) -> Self::Item<'world> {
    }
}

impl<T> Access for Read<T>
where
    T: Send + Sync + 'static,
{
    type Item<'world> = T;
    type State = ComponentId;

    fn init(ids: &mut IdRegister, set: &mut AccessSet) -> Self::State {
        todo!()
    }

    unsafe fn fetch<'world>(
        state: &'world Self::State,
        world: &'world World,
    ) -> Self::Item<'world> {
        todo!()
    }
}

impl<T: Send + Sync + 'static> Access for Write<T> {
    type Item<'world> = T;
    type State = ComponentId;

    fn init(ids: &mut IdRegister, set: &mut AccessSet) -> Self::State {
        todo!()
    }

    unsafe fn fetch<'world>(
        state: &'world Self::State,
        world: &'world World,
    ) -> Self::Item<'world> {
        todo!()
    }
}

macro_rules! impl_access_for_tuples {
    // Base case: nothing left, generate nothing.
    () => {};

    // Take the first name off the list, implement for the whole list,
    // then call ourselves again on the rest.
    ($head:ident $(, $tail:ident)*) => {
        impl<$head: Access, $($tail: Access),*> Access for ($head, $($tail,)*) {
            type Item<'w> = ($head::Item<'w>, $($tail::Item<'w>,)*);
            type State = ($head::State, $($tail::State,)*);

            fn init(ids: &mut IdRegister, set: &mut AccessSet) -> Self::State {
                ($head::init(ids, set), $($tail::init(ids, set),)*)
            }

            // Unpacks the state tuple into variables named after the type
            // parameters, since a macro can't count to write `state.0`, `state.1`.
            #[allow(non_snake_case)]
            unsafe fn fetch<'w>(state: &'w Self::State, world: &'w World) -> Self::Item<'w> {
                let ($head, $($tail,)*) = state;
                unsafe { ($head::fetch($head, world), $($tail::fetch($tail, world),)*) }
            }
        }

        impl_access_for_tuples!($($tail),*);
    };
}

impl_access_for_tuples!(A, B, C, D, E, F, G, H, I, J, K, L);

#[cfg(test)]
mod tests {
    use super::*;

    fn id(raw: u32) -> ComponentId {
        ComponentId::from_raw(raw)
    }

    fn access(reads: &[u32], writes: &[u32]) -> AccessSet {
        let mut set = AccessSet::new();
        for &r in reads {
            set.reads(id(r));
        }
        for &w in writes {
            set.writes(id(w));
        }
        set
    }

    /// Checks both directions, since a conflict between two systems doesn't
    /// depend on which one we ask.
    fn assert_conflict(a: &AccessSet, b: &AccessSet, expected: bool) {
        assert_eq!(a.conflicts(b), expected, "a.conflicts(b)");
        assert_eq!(b.conflicts(a), expected, "b.conflicts(a)");
    }

    #[test]
    fn empty_sets_do_not_conflict() {
        assert_conflict(&access(&[], &[]), &access(&[], &[]), false);
        assert_conflict(&access(&[], &[]), &access(&[0], &[1]), false);
    }

    #[test]
    fn reads_of_the_same_component_do_not_conflict() {
        assert_conflict(&access(&[0, 1], &[]), &access(&[1, 2], &[]), false);
    }

    #[test]
    fn write_conflicts_with_read_of_the_same_component() {
        assert_conflict(&access(&[], &[0]), &access(&[0], &[]), true);
    }

    #[test]
    fn write_conflicts_with_write_of_the_same_component() {
        assert_conflict(&access(&[], &[0]), &access(&[], &[0]), true);
    }

    #[test]
    fn accesses_of_different_components_do_not_conflict() {
        assert_conflict(&access(&[0], &[1]), &access(&[2], &[3]), false);
    }

    #[test]
    fn one_shared_component_is_enough_to_conflict() {
        assert_conflict(&access(&[0, 1], &[2]), &access(&[3], &[4, 1]), true);
    }

    #[test]
    fn sets_of_different_lengths_compare_correctly() {
        // One set only has low ids, the other has a high one, so the
        // underlying bit vectors have different lengths.
        assert_conflict(&access(&[0], &[]), &access(&[], &[200]), false);
        assert_conflict(&access(&[0, 200], &[]), &access(&[], &[200]), true);
        assert_conflict(&access(&[], &[200]), &access(&[200], &[]), true);
    }

    #[test]
    fn reading_the_same_component_twice_is_allowed() {
        let set = access(&[0, 0], &[]);
        assert_conflict(&set, &access(&[0], &[]), false);
        assert_conflict(&set, &access(&[], &[0]), true);
    }

    #[test]
    #[should_panic = "twice or read and write"]
    fn writing_a_component_that_is_read_panics() {
        access(&[0], &[0]);
    }

    #[test]
    #[should_panic = "already writes"]
    fn reading_a_component_that_is_written_panics() {
        let mut set = access(&[], &[0]);
        set.reads(id(0));
    }

    #[test]
    #[should_panic = "twice or read and write"]
    fn writing_the_same_component_twice_panics() {
        access(&[], &[0, 0]);
    }

    // Distinct component types for the declaration tests. `C<N>` gives as
    // many as we need for the 12-element tuple.
    struct Pos;
    struct Vel;
    struct C<const N: usize>;

    fn declared<A: Access>(ids: &mut IdRegister) -> AccessSet {
        let mut set = AccessSet::new();
        A::init(ids, &mut set);
        set
    }

    fn reads<T: 'static>(set: &AccessSet, ids: &mut IdRegister) -> bool {
        set.reads.contains(ids.id_or_register::<T>().index())
    }

    fn writes<T: 'static>(set: &AccessSet, ids: &mut IdRegister) -> bool {
        set.writes.contains(ids.id_or_register::<T>().index())
    }

    #[test]
    fn read_declares_a_read() {
        let mut ids = IdRegister::new();
        let set = declared::<Read<Pos>>(&mut ids);
        assert!(reads::<Pos>(&set, &mut ids));
        assert!(!writes::<Pos>(&set, &mut ids));
    }

    #[test]
    fn write_declares_a_write() {
        let mut ids = IdRegister::new();
        let set = declared::<Write<Pos>>(&mut ids);
        assert!(writes::<Pos>(&set, &mut ids));
        assert!(!reads::<Pos>(&set, &mut ids));
    }

    #[test]
    fn declaring_registers_the_component() {
        let mut ids = IdRegister::new();
        assert_eq!(ids.id_of::<Pos>(), None);
        declared::<Read<Pos>>(&mut ids);
        assert!(ids.id_of::<Pos>().is_some());
    }

    #[test]
    fn tuple_declares_every_element() {
        let mut ids = IdRegister::new();
        let set = declared::<(Write<Pos>, Read<Vel>)>(&mut ids);
        assert!(writes::<Pos>(&set, &mut ids));
        assert!(reads::<Vel>(&set, &mut ids));
        assert!(!reads::<Pos>(&set, &mut ids));
        assert!(!writes::<Vel>(&set, &mut ids));
    }

    #[test]
    fn tuple_registers_components_in_order() {
        let mut ids = IdRegister::new();
        declared::<(Read<Vel>, Write<Pos>)>(&mut ids);
        assert_eq!(ids.id_of::<Vel>(), Some(id(0)));
        assert_eq!(ids.id_of::<Pos>(), Some(id(1)));
    }

    #[test]
    fn one_element_tuple_declares_like_its_element() {
        let mut ids = IdRegister::new();
        let set = declared::<(Write<Pos>,)>(&mut ids);
        assert!(writes::<Pos>(&set, &mut ids));
    }

    #[test]
    fn twelve_element_tuple_declares_every_element() {
        type Twelve = (
            Read<C<0>>,
            Write<C<1>>,
            Read<C<2>>,
            Write<C<3>>,
            Read<C<4>>,
            Write<C<5>>,
            Read<C<6>>,
            Write<C<7>>,
            Read<C<8>>,
            Write<C<9>>,
            Read<C<10>>,
            Write<C<11>>,
        );
        let mut ids = IdRegister::new();
        let set = declared::<Twelve>(&mut ids);
        assert_eq!(set.reads.count(), 6);
        assert_eq!(set.writes.count(), 6);
        assert!(reads::<C<0>>(&set, &mut ids));
        assert!(writes::<C<11>>(&set, &mut ids));
    }

    #[test]
    fn nested_tuples_declare_every_element() {
        let mut ids = IdRegister::new();
        let set = declared::<(Read<Vel>, (Write<Pos>,))>(&mut ids);
        assert!(reads::<Vel>(&set, &mut ids));
        assert!(writes::<Pos>(&set, &mut ids));
    }

    #[test]
    fn declared_systems_conflict_on_shared_components() {
        let mut ids = IdRegister::new();
        let movement = declared::<(Write<Pos>, Read<Vel>)>(&mut ids);
        let render = declared::<Read<Pos>>(&mut ids);
        let steering = declared::<Read<Vel>>(&mut ids);
        assert_conflict(&movement, &render, true);
        assert_conflict(&movement, &steering, false);
        assert_conflict(&render, &steering, false);
    }

    #[test]
    #[should_panic = "already writes"]
    fn declaring_write_then_read_of_one_component_panics() {
        declared::<(Write<Pos>, Read<Pos>)>(&mut IdRegister::new());
    }

    #[test]
    #[should_panic = "twice or read and write"]
    fn declaring_read_then_write_of_one_component_panics() {
        declared::<(Read<Pos>, Write<Pos>)>(&mut IdRegister::new());
    }

    #[test]
    #[should_panic = "twice or read and write"]
    fn declaring_two_writes_of_one_component_panics() {
        declared::<(Write<Pos>, Write<Pos>)>(&mut IdRegister::new());
    }

    #[test]
    fn read_and_write_keep_their_component_id_as_state() {
        let mut ids = IdRegister::new();
        let mut set = AccessSet::new();
        let read = Read::<Vel>::init(&mut ids, &mut set);
        let write = Write::<Pos>::init(&mut ids, &mut set);
        assert_eq!(Some(read), ids.id_of::<Vel>());
        assert_eq!(Some(write), ids.id_of::<Pos>());
    }

    #[test]
    fn tuple_state_holds_each_element_state_in_order() {
        let mut ids = IdRegister::new();
        let (pos, vel) = <(Write<Pos>, Read<Vel>)>::init(&mut ids, &mut AccessSet::new());
        assert_eq!(Some(pos), ids.id_of::<Pos>());
        assert_eq!(Some(vel), ids.id_of::<Vel>());
        assert_ne!(pos, vel);
    }

    #[test]
    fn nested_tuple_state_keeps_its_shape() {
        let mut ids = IdRegister::new();
        let (vel, (pos,)) = <(Read<Vel>, (Write<Pos>,))>::init(&mut ids, &mut AccessSet::new());
        assert_eq!(Some(vel), ids.id_of::<Vel>());
        assert_eq!(Some(pos), ids.id_of::<Pos>());
    }

    #[test]
    fn unit_declares_nothing() {
        let mut ids = IdRegister::new();
        let set = declared::<()>(&mut ids);
        assert!(set.reads.is_empty());
        assert!(set.writes.is_empty());
        assert_eq!(ids.id_of::<()>(), None);
    }

    /// Test-only access whose state is `N` and whose fetch hands that state
    /// back, so we can see which state each tuple element is given.
    struct Echo<const N: u32>;

    impl<const N: u32> Access for Echo<N> {
        type Item<'w> = u32;
        type State = u32;

        fn init(_ids: &mut IdRegister, _set: &mut AccessSet) -> u32 {
            N
        }

        unsafe fn fetch<'w>(state: &'w u32, _world: &'w World) -> u32 {
            *state
        }
    }

    #[test]
    fn tuple_fetch_gives_each_element_its_own_state() {
        let world = World {};
        let state =
            <(Echo<1>, Echo<2>, Echo<3>)>::init(&mut IdRegister::new(), &mut AccessSet::new());
        let items = unsafe { <(Echo<1>, Echo<2>, Echo<3>)>::fetch(&state, &world) };
        assert_eq!(items, (1, 2, 3));
    }

    #[test]
    fn nested_tuple_fetch_keeps_its_shape() {
        type Nested = (Echo<1>, (Echo<2>, Echo<3>), (Echo<4>,));
        let world = World {};
        let state = Nested::init(&mut IdRegister::new(), &mut AccessSet::new());
        let items = unsafe { Nested::fetch(&state, &world) };
        assert_eq!(items, (1, (2, 3), (4,)));
    }
}
