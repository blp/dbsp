use std::{
    collections::VecDeque,
    iter::once,
    panic::{catch_unwind, RefUnwindSafe},
    sync::{Arc, Condvar, Mutex},
    thread::{self, scope},
};

use clap::{Parser, ValueEnum};
use rand::{thread_rng, Rng};

/// Model test options.
#[derive(Parser)]
pub struct ModelTestOptions {
    /// Number of threads to use.
    #[arg(short, long, default_value_t = 4)]
    threads: usize,

    /// Maximum search depth to explore.  The initial state has depth 0.
    #[arg(long)]
    max_depth: Option<usize>,

    /// Stop after visiting the specified number of states.
    #[arg(long, default_value_t = 100)]
    max_states: usize,

    /// Stop after encountering the specified number of errors.
    #[arg(long, default_value_t = 1)]
    max_errors: usize,

    /// Limit queue to N states.
    #[arg(long, default_value_t = 10000)]
    queue_limit: usize,

    /// How to drop states when queue overflows.
    #[arg(long, value_enum, default_value = "random")]
    queue_drop: QueueDrop,

    /// Verbosity level before an error.
    #[arg(short, long, default_value_t = 1)]
    verbosity: usize,

    /// Verbosity level for replaying failure cases.
    #[arg(short, long, default_value_t = 3)]
    failure_verbosity: usize,

    /// A specific path to run, in place of searching the action space.
    #[arg(long)]
    path: Option<String>,

    /// Search strategy.
    #[arg(long, value_enum, default_value = "bfs")]
    strategy: Strategy,
}

#[derive(Clone, ValueEnum)]
pub enum Strategy {
    /// Breadth-first search.
    #[value(name = "bfs")]
    BreadthFirst,

    /// Depth-first searching.
    #[value(name = "dfs")]
    DepthFirst,

    /// Random search.
    Random,
}

/// How to drop states when queue overflows.
#[derive(Clone, ValueEnum)]
pub enum QueueDrop {
    /// Drop most recently added state.
    Newest,

    /// Drop least recently added state.
    Oldest,

    /// Drop a random state.
    Random,
}

#[derive(Debug)]
pub enum MutateError {
    NoSuchStep(usize),
    Failure,
}

fn test_state<F>(f: F, path: &Path, verbosity: usize) -> Result<usize, MutateError>
where
    F: Fn(&str, &[usize], usize) -> Result<usize, MutateError> + RefUnwindSafe + Send + Sync,
{
    if verbosity > 0 {
        println!("Testing path {path:?}");
    }

    let name: String = once("path".into())
        .chain(path.iter().map(|action| format!("_{action}")))
        .collect();
    let result = catch_unwind(|| f(&name, path, verbosity).unwrap());
    match result {
        Ok(n) => {
            if verbosity > 1 {
                println!("    Path {path:?} succeeded.");
            }
            Ok(n)
        }
        Err(cause) => {
            println!("    Path {path:?} panicked with: {cause:?}");
            Err(MutateError::Failure)
        }
    }
}

type Path = Vec<usize>;

struct TestState {
    // States to be tested.
    queue: VecDeque<Path>,

    // Number of states tested so far.
    n_states: usize,

    // Failed states.
    failures: Vec<Path>,

    // Number of threads waiting for the queue to become nonempty.
    n_waiters: usize,
}

impl TestState {
    fn new() -> Self {
        TestState {
            queue: once(Vec::new()).collect(),
            n_states: 0,
            failures: Vec::new(),
            n_waiters: 0,
        }
    }

    fn should_exit(&self, opts: &ModelTestOptions) -> bool {
        self.failures.len() >= opts.max_errors || self.n_states >= opts.max_states
    }

    fn queue_state(&mut self, opts: &ModelTestOptions, actions: Path) {
        if self.queue.len() >= opts.queue_limit {
            match opts.queue_drop {
                QueueDrop::Newest => return,
                QueueDrop::Oldest => match opts.strategy {
                    Strategy::BreadthFirst => {
                        self.queue.pop_front();
                    }
                    Strategy::DepthFirst | Strategy::Random => {
                        self.queue.pop_back();
                    }
                },
                QueueDrop::Random => {
                    self.queue
                        .remove(thread_rng().gen_range(0..self.queue.len()));
                }
            }
        }
        match opts.strategy {
            Strategy::BreadthFirst => self.queue.push_back(actions),
            Strategy::DepthFirst => self.queue.push_front(actions),
            Strategy::Random => {
                self.queue.push_front(actions);
                self.queue
                    .swap(0, thread_rng().gen_range(0..self.queue.len()));
            }
        };
    }

    fn next_action(
        state: &Mutex<Self>,
        condvar: &Condvar,
        opts: &ModelTestOptions,
    ) -> Result<Path, ()> {
        let mut state = state.lock().unwrap();
        loop {
            if state.should_exit(opts) {
                state.n_waiters += 1;
                return Err(());
            }
            if let Some(actions) = state.queue.pop_front() {
                state.n_states += 1;
                return Ok(actions);
            }
            state.n_waiters += 1;
            if state.n_waiters >= opts.threads {
                condvar.notify_all();
                return Err(());
            }
            state = condvar.wait(state).unwrap();
            state.n_waiters -= 1;
        }
    }
}

/// Explores the state space of a deterministic model, provided as function `f`,
/// according to the options in `opts`.
///
/// The model is state based.  In each state, the model must be able to execute
/// a finite number (in practice, a small number) of actions that transition to
/// another state, and assign each of these actions an integer in `0...N` where
/// `N` is the number of actions in that state.  The model must have a single
/// initial state.
///
/// Function `f` runs the model.  It has three parameters:
///
///   - `name: &str`: An identifying name.
///
///   - `actions: &Vec<usize>`: A sequence of actions to take.
///
///   - `verbosity: usize`: A verbosity level.
///
/// `f` should initialize itself to the initial state, execute the actions in
/// `actions` in order, and return `N`, the number of actions that are available
/// in the final state.  The driver code will then queue up `[actions, 0]`,
/// `[actions, 1]`, through [actions, N - 1]` for testing.
///
/// If `f` detects some inconsistency while it is executing the actionsg it is
/// given, it should panic.  The driver reports the error.  It will also, by
/// default, automatically rerun the failing tests at a higher verbosity level.
///
/// `verbosity` should determine how much `f` should write to the console.  For
/// `verbosity` of 0 or 1, it should only print errors due to inconsistency.
/// For level 2, it should print a one-line description of each action that
/// it executes, and other information of the same level of importance.  At
/// levels 3 and higher, it may print other less important information as well.
pub fn test_model<F>(opts: &ModelTestOptions, f: F)
where
    F: Fn(&str, &[usize], usize) -> Result<usize, MutateError> + RefUnwindSafe + Send + Sync,
{
    if let Some(ref path) = opts.path {
        let path: Path = path
            .split([' ', ',', '[', ']', '_'])
            .filter(|s| !s.is_empty())
            .map(|s| s.parse().unwrap())
            .collect();
        test_state(f, &path, opts.verbosity).unwrap();
        return;
    }

    if opts.verbosity > 0 {
        print!(
            "Running up to {} tests using {} threads",
            opts.max_states, opts.threads
        );
        if let Some(max_depth) = opts.max_depth {
            print!(" with search depth limited to {max_depth}");
        }
        println!();
    }

    let pair = Arc::new((Mutex::new(TestState::new()), Condvar::new()));
    let f = &f;
    scope(|s| {
        for i in 0..opts.threads {
            let pair = pair.clone();
            thread::Builder::new()
                .name(format!("test_model_{i}"))
                .spawn_scoped(s, move || {
                    let (state, wakeup) = &*pair;
                    while let Ok(actions) = TestState::next_action(state, wakeup, opts) {
                        let Ok(n) = test_state(f, &actions, opts.verbosity) else {
                            state.lock().unwrap().failures.push(actions.clone());
                            wakeup.notify_all();
                            continue;
                        };

                        if opts
                            .max_depth
                            .map_or(true, |max_depth| actions.len() < max_depth)
                        {
                            let mut state = state.lock().unwrap();
                            for new_action in 0..n {
                                state.queue_state(
                                    opts,
                                    actions.iter().copied().chain(once(new_action)).collect(),
                                );
                            }
                            wakeup.notify_all();
                        }
                    }
                })
                .unwrap();
        }
    });

    let state = Arc::into_inner(pair).unwrap().0.into_inner().unwrap();
    if !state.failures.is_empty() {
        println!("The following paths failed:");
        for failure in &state.failures {
            println!("    {failure:?}");
        }

        if opts.failure_verbosity > opts.verbosity {
            println!(
                "Rerunning failures at verbosity level {}:",
                opts.failure_verbosity
            );
            for failure in &state.failures {
                let _ = test_state(f, failure, opts.failure_verbosity);
            }
        }

        panic!("Paths failed: {:?}", state.failures);
    }
}
