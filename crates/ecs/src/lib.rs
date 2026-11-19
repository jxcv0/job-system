use job_system::Scope;
use std::sync::atomic::{AtomicU32, Ordering};

mod access;
mod components;

struct World {}

type SystemFunction = dyn Fn(&Scope, &World) + Send + Sync + 'static;

struct System {
    /// The number of parents this system is waiting on before it can be scheduled
    parents: u32,
    /// We reset this counter to self.parents at the end of every frame
    remaining: AtomicU32,
    /// The indices of the child systems that can only be scheduled once this System has run
    children: Vec<u32>,
    /// The function this system runs
    run: Box<SystemFunction>,
}

struct Schedule {
    systems: Vec<System>,
}

// Run a system at index i
// fn run_system<'s>(graph: &'s Schedule, i: u32, s: &'s Scope<'s, '_>, world: &'s World) {
//     let sys = &graph.systems[i as usize];
//     (sys.run)(s, world);
//     for &child in &sys.children {
//         if graph.systems[child as usize].remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
//             s.spawn_scoped(move |s| run_system(graph, child, s, world));
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
}
