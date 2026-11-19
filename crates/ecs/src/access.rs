use crate::components::ComponentId;

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
        todo!()
    }

    pub(crate) fn writes(&mut self, id: ComponentId) {
        todo!()
    }

    pub(crate) fn conflicts(&self, other: &AccessSet) {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
