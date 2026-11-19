use std::marker::PhantomData;

use crate::{World, components::{ComponentId, IdRegister}};

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
    // What the system fn closure recieves
    type Item<'world>;

    // On build() say which components we touch
    fn declare(ids: &mut IdRegister, set: &mut AccessSet);

    /// Access the colums in component storage
    fn fetch<'world>(ids: &IdRegister, world: &'world World) -> Self::Item<'world>;
}

pub(crate) struct Read<T>(PhantomData<T>);
pub(crate) struct Write<T>(PhantomData<T>);

impl<T> Access for Read<T> {
    type Item<'world> = T;

    fn declare(ids: &mut IdRegister, set: &mut AccessSet) {
        todo!()
    }

    fn fetch<'world>(ids: &IdRegister, world: &'world World) -> Self::Item<'world> {
        todo!()
    }
}

impl<T> Access for Write<T> {
    type Item<'world> = T;

    fn declare(ids: &mut IdRegister, set: &mut AccessSet) {
        todo!()
    }

    fn fetch<'world>(ids: &IdRegister, world: &'world World) -> Self::Item<'world> {
        todo!()
    }
}

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
}
