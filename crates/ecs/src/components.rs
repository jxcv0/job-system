use std::{any::TypeId, collections::HashMap};

#[derive(Debug, Copy, Clone, PartialEq)]
pub(crate) struct ComponentId(u32);

impl ComponentId {
    #[cfg(test)]
    pub(crate) fn from_raw(raw: u32) -> Self {
        Self(raw)
    }

    pub(crate) fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Default)]
pub(crate) struct IdRegister {
    inner: HashMap<TypeId, ComponentId>,
}
impl IdRegister {
    pub(crate) fn new() -> Self {
        Default::default()
    }

    pub(crate) fn id_or_register<T: 'static>(&mut self) -> ComponentId {
        let next = ComponentId(self.inner.len() as u32);
        *self.inner.entry(TypeId::of::<T>()).or_insert(next)
    }

    pub(crate) fn id_of<T: 'static>(&self) -> Option<ComponentId> {
        self.inner.get(&TypeId::of::<T>()).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_gives_dense_stable_ids() {
        let mut r = IdRegister::new();
        assert_eq!(r.id_or_register::<u8>(), ComponentId(0));
        assert_eq!(r.id_or_register::<u16>(), ComponentId(1));
        assert_eq!(r.id_or_register::<u8>(), ComponentId(0));
        assert_eq!(r.id_or_register::<u16>(), ComponentId(1));
        assert_eq!(r.id_of::<u32>(), None);
        assert_eq!(r.id_of::<u16>(), Some(ComponentId(1)));
        assert_eq!(r.id_or_register::<u32>(), ComponentId(2));
    }
}
