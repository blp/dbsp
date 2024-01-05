use crate::{
    algebra::{DefaultSemigroup, GroupValue, HasOne, HasZero, IndexedZSet, MulByRef, ZRingValue},
    circuit::{
        operator_traits::{Operator, QuaternaryOperator},
        Scope,
    },
    operator::{
        time_series::{
            radix_tree::{PartitionedRadixTreeReader, RadixTreeCursor},
            range::{Range, RangeCursor, Ranges, RelRange},
            OrdPartitionedIndexedZSet, PartitionCursor, PartitionedBatchReader,
            PartitionedIndexedZSet, RelOffset,
        },
        trace::{TraceBound, TraceBounds, TraceFeedback},
        Aggregator, Avg, FilterMap,
    },
    trace::{BatchReader, Builder, Cursor, Spine},
    Circuit, DBData, DBWeight, RootCircuit, Stream,
};
use num::{Bounded, PrimInt};
use std::{
    borrow::Cow,
    fmt::Debug,
    marker::PhantomData,
    ops::{Div, Neg},
};

// TODO: `Default` trait bounds in this module are due to an implementation
// detail and can in principle be avoided.

pub type OrdPartitionedOverStream<PK, TS, A, R> =
    Stream<RootCircuit, OrdPartitionedIndexedZSet<PK, TS, Option<A>, R>>;

/// `Aggregator` object that computes a linear aggregation function.
// TODO: we need this because we currently compute linear aggregates
// using the same algorithm as general aggregates.  Additional performance
// gains can be obtained with an optimized implementation of radix trees
// for linear aggregates (specifically, updating a node when only
// some of its children have changed can be done without computing
// the sum of all children from scratch).
struct LinearAggregator<V, R, A, O, F, OF> {
    f: F,
    output_func: OF,
    phantom: PhantomData<(V, R, A, O)>,
}

impl<V, R, A, O, F, OF> Clone for LinearAggregator<V, R, A, O, F, OF>
where
    F: Clone,
    OF: Clone,
{
    fn clone(&self) -> Self {
        Self {
            f: self.f.clone(),
            output_func: self.output_func.clone(),
            phantom: PhantomData,
        }
    }
}

impl<V, R, A, O, F, OF> LinearAggregator<V, R, A, O, F, OF> {
    fn new(f: F, output_func: OF) -> Self {
        Self {
            f,
            output_func,
            phantom: PhantomData,
        }
    }
}

impl<V, R, A, O, F, OF> Aggregator<V, (), R> for LinearAggregator<V, R, A, O, F, OF>
where
    V: DBData,
    R: DBWeight + ZRingValue,
    A: DBData + MulByRef<R, Output = A> + GroupValue,
    O: DBData,
    F: Fn(&V) -> A + Clone + 'static,
    OF: Fn(A) -> O + Clone + 'static,
{
    type Accumulator = A;
    type Output = O;

    type Semigroup = DefaultSemigroup<A>;

    fn aggregate<C>(&self, cursor: &mut C) -> Option<A>
    where
        C: Cursor<V, (), (), R>,
    {
        let mut res: Option<A> = None;

        while cursor.key_valid() {
            let w = cursor.weight();
            let new = (self.f)(cursor.key()).mul_by_ref(&w);
            res = match res {
                None => Some(new),
                Some(old) => Some(old + new),
            };
            cursor.step_key();
        }
        res
    }

    fn finalize(&self, accumulator: Self::Accumulator) -> Self::Output {
        (self.output_func)(accumulator)
    }
}

impl<B> Stream<RootCircuit, B>
where
    B: IndexedZSet,
{
    /// Similar to
    /// [`partitioned_rolling_aggregate`](`Stream::partitioned_rolling_aggregate`),
    /// but uses `warerline` to bound its memory footprint.
    ///
    /// Splits the input stream into non-overlapping
    /// partitions using `partition_func` and for each input record
    /// computes an aggregate over a relative time range (e.g., the
    /// last three months) within its partition.  Outputs the contents
    /// of the input stream extended with the value of the aggregate.
    ///
    /// This operator is incremental and will update previously
    /// computed outputs affected by new data.  For example,
    /// a data point arriving out-of-order may affect previously
    /// computed rolling aggregate values at future times.
    ///
    /// The `warerline` stream bounds the out-of-orderedness of the input
    /// data by providing a monotonically growing lower bound on
    /// timestamps that can appear in the input stream.  The operator
    /// does not expect inputs with timestamps smaller than the current
    /// warerline.  The `warerline` value is used to bound the amount of
    /// state maintained by the operator.
    ///
    /// # Background
    ///
    /// The rolling aggregate operator is typically applied to time series data
    /// with bounded out-of-orderedness, i.e, having seen a timestamp `ts` in
    /// the input stream, the operator will never observe a timestamp
    /// smaller than `ts - b` for some bound `b`.  This in turn means that
    /// the value of the aggregate will remain constant for timestamps that
    /// only depend on times `< ts - b`.  Hence, we do not need to maintain
    /// the state needed to recompute these aggregates, which allows us to
    /// bound the amount of state maintained by this operator.
    ///
    /// The bound `ts - b` is known as "warerline" and can be computed, e.g., by
    /// the [`waterline_monotonic`](`Stream::waterline_monotonic`) operator.
    ///
    /// # Arguments
    ///
    /// * `self` - time series data indexed by time.
    /// * `warerline` - monotonically growing lower bound on timestamps in the
    ///   input stream.
    /// * `partition_func` - function used to split inputs into non-overlapping
    ///   partitions indexed by partition key of type `PK`.
    /// * `aggregator` - aggregator used to summarize values within the relative
    ///   time range `range` of each input timestamp.
    /// * `range` - relative time range to aggregate over.
    pub fn partitioned_rolling_aggregate_with_waterline<PK, TS, V, Agg, PF>(
        &self,
        warerline: &Stream<RootCircuit, TS>,
        partition_func: PF,
        aggregator: Agg,
        range: RelRange<TS>,
    ) -> OrdPartitionedOverStream<PK, TS, Agg::Output, B::R>
    where
        B: IndexedZSet<Key = TS>,
        Self: for<'a> FilterMap<RootCircuit, ItemRef<'a> = (&'a B::Key, &'a B::Val), R = B::R>,
        B::R: ZRingValue,
        PK: DBData,
        PF: Fn(&B::Val) -> (PK, V) + Clone + 'static,
        Agg: Aggregator<V, (), B::R>,
        Agg::Accumulator: Default,
        TS: DBData + PrimInt,
        V: DBData,
    {
        self.circuit()
            .region("partitioned_rolling_aggregate_with_warerline", || {
                // Shift the aggregation window so that its right end is at 0.
                let shifted_range =
                    RelRange::new(range.from - range.to, RelOffset::Before(TS::zero()));

                // Trace bound used inside `partitioned_rolling_aggregate_inner` to
                // bound its output trace.  This is the same bound we use to construct
                // the input window here.
                let bound: TraceBound<(TS, Option<Agg::Output>)> = TraceBound::new();
                let bound_clone = bound.clone();

                // Restrict the input stream to the `[lb -> ∞)` time window,
                // where `lb = warerline - (range.to - range.from)` is the lower
                // bound on input timestamps that may be used to compute
                // changes to the rolling aggregate operator.
                let bounds = warerline.apply(move |wm| {
                    let lower = shifted_range
                        .range_of(wm)
                        .map(|range| range.from)
                        .unwrap_or_else(|| Bounded::min_value());
                    bound_clone.set((lower, None));
                    (lower, Bounded::max_value())
                });
                let window = self.window(&bounds);

                // Now that we've truncated old inputs, which required the
                // input stream to be indexed by time, we can re-index it
                // by partition id.
                let partition_func_clone = partition_func.clone();

                let partitioned_window = window.map_index(move |(ts, v)| {
                    let (partition_key, val) = partition_func_clone(v);
                    (partition_key, (*ts, val))
                });
                let partitioned_self = self.map_index(move |(ts, v)| {
                    let (partition_key, val) = partition_func(v);
                    (partition_key, (*ts, val))
                });

                partitioned_self.partitioned_rolling_aggregate_inner(
                    &partitioned_window,
                    aggregator,
                    range,
                    bound,
                )
            })
    }
}

impl<B> Stream<RootCircuit, B> {
    /// Rolling aggregate of a partitioned stream over time range.
    ///
    /// For each record in the input stream, computes an aggregate
    /// over a relative time range (e.g., the last three months).
    /// Outputs the contents of the input stream extended with the
    /// value of the aggregate.
    ///
    /// For each input record `(p, (ts, v))`, rolling aggregation finds all the
    /// records `(p, (ts2, x))` such that `ts2` is in `range(ts)`, applies
    /// `aggregator` across these records to obtain a finalized value `f`,
    /// and outputs `(p, (ts, f))`.
    ///
    /// This operator is incremental and will update previously
    /// computed outputs affected by new data.  For example,
    /// a data point arriving out-of-order may affect previously
    /// computed rolling aggregate value at future times.
    pub fn partitioned_rolling_aggregate<TS, V, Agg>(
        &self,
        aggregator: Agg,
        range: RelRange<TS>,
    ) -> OrdPartitionedOverStream<B::Key, TS, Agg::Output, B::R>
    where
        B: PartitionedIndexedZSet<TS, V>,
        B::R: ZRingValue,
        Agg: Aggregator<V, (), B::R>,
        Agg::Accumulator: Default,
        TS: DBData + PrimInt,
        V: DBData,
    {
        self.partitioned_rolling_aggregate_generic::<TS, V, Agg, _>(aggregator, range)
    }

    /// Like [`Self::partitioned_rolling_aggregate`], but can return any
    /// batch type.
    pub fn partitioned_rolling_aggregate_generic<TS, V, Agg, O>(
        &self,
        aggregator: Agg,
        range: RelRange<TS>,
    ) -> Stream<RootCircuit, O>
    where
        B: PartitionedIndexedZSet<TS, V>,
        B::R: ZRingValue,
        Agg: Aggregator<V, (), B::R>,
        Agg::Accumulator: Default,
        O: PartitionedIndexedZSet<TS, Option<Agg::Output>, Key = B::Key, R = B::R>,
        TS: DBData + PrimInt,
        V: DBData,
    {
        // ```
        //                  ┌───────────────┐   input_trace
        //      ┌──────────►│integrate_trace├──────────────┐                              output
        //      │           └───────────────┘              │                           ┌────────────────────────────────────►
        //      │                                          ▼                           │
        // self │    ┌──────────────────────────┐  tree  ┌───────────────────────────┐ │  ┌──────────────────┐ output_trace
        // ─────┼───►│partitioned_tree_aggregate├───────►│PartitionedRollingAggregate├─┴──┤UntimedTraceAppend├────────┐
        //      │    └──────────────────────────┘        └───────────────────────────┘    └──────────────────┘        │
        //      │                                          ▲               ▲                 ▲                        │
        //      └──────────────────────────────────────────┘               │                 │                        │
        //                                                                 │               ┌─┴──┐                     │
        //                                                                 └───────────────┤Z^-1│◄────────────────────┘
        //                                                                   delayed_trace └────┘
        // ```
        self.circuit().region("partitioned_rolling_aggregate", || {
            self.partitioned_rolling_aggregate_inner(self, aggregator, range, TraceBound::new())
        })
    }

    #[doc(hidden)]
    pub fn partitioned_rolling_aggregate_inner<TS, V, Agg, O>(
        &self,
        self_window: &Self,
        aggregator: Agg,
        range: RelRange<TS>,
        bound: TraceBound<(TS, Option<Agg::Output>)>,
    ) -> Stream<RootCircuit, O>
    where
        B: PartitionedIndexedZSet<TS, V>,
        B::R: ZRingValue,
        Agg: Aggregator<V, (), B::R>,
        Agg::Accumulator: Default,
        O: PartitionedIndexedZSet<TS, Option<Agg::Output>, Key = B::Key, R = B::R>,
        TS: DBData + PrimInt,
        V: DBData,
    {
        let circuit = self.circuit();
        let stream = self.shard();
        let stream_window = self_window.shard();

        // Build the radix tree over the bounded window.
        let tree = stream_window
            .partitioned_tree_aggregate::<TS, V, Agg>(aggregator.clone())
            .integrate_trace();
        let input_trace = stream_window.integrate_trace();

        // Truncate timestamps `< bound` in the output trace.
        let bounds = TraceBounds::new();
        bounds.add_key_bound(TraceBound::new());
        bounds.add_val_bound(bound);

        let feedback = circuit.add_integrate_trace_feedback::<Spine<O>>(bounds);

        let output = circuit
            .add_quaternary_operator(
                <PartitionedRollingAggregate<TS, V, Agg>>::new(range, aggregator),
                &stream,
                &input_trace,
                &tree,
                &feedback.delayed_trace,
            )
            .mark_distinct()
            .mark_sharded();

        feedback.connect(&output);

        output
    }

    /// A version of [`Self::partitioned_rolling_aggregate`] optimized for
    /// linear aggregation functions.  For each input record `(p, (ts, v))`,
    /// it finds all the records `(p, (ts2, x))` such that `ts2` is in
    /// `range.range_of(ts)`, computes the sum `s` of `f(x)` across these
    /// records, and outputs `(p, (ts, Some(output_func(s))))`.
    ///
    /// Output records from linear aggregation contain an `Option` type because
    /// there might be no records matching `range.range_of(ts)`.  If `range`
    /// contains (relative) time 0, this never happens (because the record
    /// containing `ts` itself is always a match), so in that case the
    /// caller can safely `unwrap()` the `Option`.
    ///
    /// In rolling aggregation, the number of output records matches the number
    /// of input records.
    ///
    /// This method only works for linear aggregation functions `f`, i.e.,
    /// functions that satisfy `f(a+b) = f(a) + f(b)`.  It will produce
    /// incorrect results if `f` is not linear.
    pub fn partitioned_rolling_aggregate_linear<TS, V, A, O, F, OF>(
        &self,
        f: F,
        output_func: OF,
        range: RelRange<TS>,
    ) -> OrdPartitionedOverStream<B::Key, TS, O, B::R>
    where
        B: PartitionedIndexedZSet<TS, V>,
        B::R: ZRingValue,
        A: DBData + MulByRef<B::R, Output = A> + GroupValue + Default,
        F: Fn(&V) -> A + Clone + 'static,
        OF: Fn(A) -> O + Clone + 'static,
        TS: DBData + PrimInt,
        V: DBData,
        O: DBData,
    {
        let aggregator = LinearAggregator::new(f, output_func);
        self.partitioned_rolling_aggregate_generic::<TS, V, _, _>(aggregator, range)
    }

    /// Like [`Self::partitioned_rolling_aggregate_linear`], but can return any
    /// batch type.
    pub fn partitioned_rolling_aggregate_linear_generic<TS, V, A, O, F, OF, Out>(
        &self,
        f: F,
        output_func: OF,
        range: RelRange<TS>,
    ) -> Stream<RootCircuit, Out>
    where
        B: PartitionedIndexedZSet<TS, V>,
        B::R: ZRingValue,
        A: DBData + MulByRef<B::R, Output = A> + GroupValue + Default,
        F: Fn(&V) -> A + Clone + 'static,
        OF: Fn(A) -> O + Clone + 'static,
        TS: DBData + PrimInt,
        V: DBData,
        O: DBData,
        Out: PartitionedIndexedZSet<TS, Option<O>, Key = B::Key, R = B::R>,
    {
        let aggregator = LinearAggregator::new(f, output_func);
        self.partitioned_rolling_aggregate_generic::<TS, V, _, _>(aggregator, range)
    }

    /// Incremental rolling average.
    ///
    /// For each input record, it computes the average of the values in records
    /// in the same partition in the time range specified by `range`.
    pub fn partitioned_rolling_average<TS, V>(
        &self,
        range: RelRange<TS>,
    ) -> OrdPartitionedOverStream<B::Key, TS, V, B::R>
    where
        B: PartitionedIndexedZSet<TS, V>,
        B::R: ZRingValue + Default,
        TS: DBData + PrimInt,
        V: DBData + From<B::R> + GroupValue + Default + MulByRef<Output = V> + Div<Output = V>,
        // This bound is only here to prevent conflict with `MulByRef<Present>` :(
        <B as BatchReader>::R: From<i8>,
    {
        self.partitioned_rolling_average_generic(range)
    }

    pub fn partitioned_rolling_average_generic<TS, V, Out>(
        &self,
        range: RelRange<TS>,
    ) -> Stream<RootCircuit, Out>
    where
        B: PartitionedIndexedZSet<TS, V>,
        B::R: ZRingValue + Default,
        TS: DBData + PrimInt,
        V: DBData + From<B::R> + GroupValue + Default + MulByRef<Output = V> + Div<Output = V>,
        // This bound is only here to prevent conflict with `MulByRef<Present>` :(
        <B as BatchReader>::R: From<i8>,
        Out: PartitionedIndexedZSet<TS, Option<V>, Key = B::Key, R = B::R>,
    {
        self.partitioned_rolling_aggregate_linear_generic(
            move |v| Avg::new(v.clone(), B::R::one()),
            |avg| avg.compute_avg().unwrap(),
            range,
        )
    }
}

/// Quaternary operator that implements the internals of
/// `partitioned_rolling_aggregate`.
///
/// * Input stream 1: updates to the time series.  Used to identify affected
///   partitions and times.
/// * Input stream 2: trace containing the accumulated time series data.
/// * Input stream 3: trace containing the partitioned radix tree over the input
///   time series.
/// * Input stream 4: trace of previously produced outputs.  Used to compute
///   retractions.
struct PartitionedRollingAggregate<TS, V, Agg> {
    range: RelRange<TS>,
    aggregator: Agg,
    phantom: PhantomData<V>,
}

impl<TS, V, Agg> PartitionedRollingAggregate<TS, V, Agg> {
    fn new(range: RelRange<TS>, aggregator: Agg) -> Self {
        Self {
            range,
            aggregator,
            phantom: PhantomData,
        }
    }

    fn affected_ranges<R, C>(&self, delta_cursor: &mut C) -> Ranges<TS>
    where
        C: Cursor<TS, V, (), R>,
        TS: PrimInt + Debug,
    {
        let mut affected_ranges = Ranges::new();
        let mut delta_ranges = Ranges::new();

        while delta_cursor.key_valid() {
            println!("affected range of {:?}", delta_cursor.key());
            if let Some(range) = self.range.affected_range_of(delta_cursor.key()) {
                affected_ranges.push_monotonic(range);
            }
            // If `delta_cursor.key()` is a new key that doesn't yet occur in the input
            // z-set, we need to compute its aggregate even if it is outside
            // affected range.
            delta_ranges.push_monotonic(Range::new(*delta_cursor.key(), *delta_cursor.key()));
            delta_cursor.step_key();
        }

        affected_ranges.merge(&delta_ranges)
    }
}

impl<TS, V, Agg> Operator for PartitionedRollingAggregate<TS, V, Agg>
where
    TS: 'static,
    V: 'static,
    Agg: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("PartitionedRollingAggregate")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

fn print_cursor<C: Cursor<K, V, T, R> + Clone, K: Debug, V, T, R>(name: &str, cursor: &C) {
    print_cursor_destructively(name, cursor.clone());
}

fn print_cursor_destructively<C: Cursor<K, V, T, R>, K: Debug, V, T, R>(name: &str, mut cursor: C) {
    println!("{name}:");
    while cursor.key_valid() {
        println!("    {:?}", cursor.key());
        cursor.step_key();
    }
}

impl<TS, V, Agg, B, T, RT, OT, O> QuaternaryOperator<B, T, RT, OT, O>
    for PartitionedRollingAggregate<TS, V, Agg>
where
    TS: DBData + PrimInt,
    V: DBData,
    Agg: Aggregator<V, (), B::R>,
    B: PartitionedBatchReader<TS, V> + Clone,
    B::R: ZRingValue,
    T: PartitionedBatchReader<TS, V, Key = B::Key, R = B::R> + Clone,
    RT: PartitionedRadixTreeReader<TS, Agg::Accumulator, Key = B::Key> + Clone,
    OT: PartitionedBatchReader<TS, Option<Agg::Output>, Key = B::Key, R = B::R> + Clone,
    O: IndexedZSet<Key = B::Key, Val = (TS, Option<Agg::Output>), R = B::R>,
{
    fn eval<'a>(
        &mut self,
        input_delta: Cow<'a, B>,
        input_trace: Cow<'a, T>,
        radix_tree: Cow<'a, RT>,
        output_trace: Cow<'a, OT>,
    ) -> O {
        let mut delta_cursor = input_delta.cursor();
        let mut output_trace_cursor = output_trace.cursor();
        let mut input_trace_cursor = input_trace.cursor();
        let mut tree_cursor = radix_tree.cursor();

        let mut retraction_builder = O::Builder::new_builder(());
        let mut insertion_builder = O::Builder::with_capacity((), input_delta.len());

        // println!("delta: {input_delta:#x?}");
        // println!("radix tree: {radix_tree:#x?}");
        // println!("aggregate_range({range:x?})");
        // let mut treestr = String::new();
        // radix_tree.cursor().format_tree(&mut treestr).unwrap();
        // println!("tree: {treestr}");
        // tree_partition_cursor.rewind_keys();

        // Iterate over affected partitions.
        while delta_cursor.key_valid() {
            // Compute affected intervals using `input_delta`.
            let ranges = self.affected_ranges(&mut PartitionCursor::new(&mut delta_cursor));
            println!(
                "affected_ranges for partition {:?}: {ranges:?}",
                delta_cursor.key()
            );

            // Clear old outputs.
            output_trace_cursor.seek_key(delta_cursor.key());
            if output_trace_cursor.key_valid() && output_trace_cursor.key() == delta_cursor.key() {
                let mut range_cursor = RangeCursor::new(
                    PartitionCursor::new(&mut output_trace_cursor),
                    ranges.clone(),
                );
                while range_cursor.key_valid() {
                    while range_cursor.val_valid() {
                        let weight = range_cursor.weight();
                        if !weight.is_zero() {
                            println!(
                                "retract: ({:?}, ({:?}, {:?})) ",
                                delta_cursor.key(),
                                range_cursor.key(),
                                range_cursor.val()
                            );
                            retraction_builder.push((
                                O::item_from(
                                    delta_cursor.key().clone(),
                                    (*range_cursor.key(), range_cursor.val().clone()),
                                ),
                                weight.neg(),
                            ));
                        }
                        range_cursor.step_val();
                    }
                    range_cursor.step_key();
                }
            };

            // Compute new outputs.
            input_trace_cursor.seek_key(delta_cursor.key());
            tree_cursor.seek_key(delta_cursor.key());

            if input_trace_cursor.key_valid() && input_trace_cursor.key() == delta_cursor.key() {
                debug_assert!(tree_cursor.key_valid());
                debug_assert_eq!(tree_cursor.key(), delta_cursor.key());

                let mut tree_partition_cursor = PartitionCursor::new(&mut tree_cursor);
                let mut input_range_cursor =
                    RangeCursor::new(PartitionCursor::new(&mut input_trace_cursor), ranges);

                // For all affected times, seek them in `input_trace`, compute aggregates using
                // using radix_tree.
                while input_range_cursor.key_valid() {
                    let range = self.range.range_of(input_range_cursor.key());
                    tree_partition_cursor.rewind_keys();

                    // println!("aggregate_range({range:x?})");
                    // let mut treestr = String::new();
                    // tree_partition_cursor.format_tree(&mut treestr).unwrap();
                    // println!("tree: {treestr}");
                    // tree_partition_cursor.rewind_keys();

                    while input_range_cursor.val_valid() {
                        // Generate output update.
                        if !input_range_cursor.weight().le0() {
                            let agg = range.clone().and_then(|range| {
                                tree_partition_cursor
                                    .aggregate_range::<Agg::Semigroup>(&range)
                                    .map(|acc| self.aggregator.finalize(acc))
                            });
                            println!(
                                "key: {:?}, range: {:?}, agg: {:?}",
                                input_range_cursor.key(),
                                range,
                                agg
                            );

                            insertion_builder.push((
                                O::item_from(
                                    delta_cursor.key().clone(),
                                    (*input_range_cursor.key(), agg),
                                ),
                                HasOne::one(),
                            ));
                            break;
                        }

                        input_range_cursor.step_val();
                    }

                    input_range_cursor.step_key();
                }
            }

            delta_cursor.step_key();
        }

        let retractions = retraction_builder.done();
        let insertions = insertion_builder.done();
        retractions.add(insertions)
    }
}

#[cfg(test)]
mod test {
    use std::{
        fs::File,
        io::Write,
        process::exit,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use crate::{
        algebra::DefaultSemigroup,
        operator::{
            time_series::{
                range::{Range, RelOffset, RelRange},
                rolling_aggregate::the_trace,
                PartitionCursor,
            },
            trace::TraceBound,
            FilterMap, Fold,
        },
        trace::{Batch, BatchReader, Cursor},
        CircuitHandle, CollectionHandle, IndexedZSet, OrdIndexedZSet, OutputHandle, RootCircuit,
        Stream,
    };
    use size_of::SizeOf;

    type DataBatch = OrdIndexedZSet<u64, (u64, i64), isize>;
    type DataStream = Stream<RootCircuit, DataBatch>;
    type OutputBatch = OrdIndexedZSet<u64, (u64, Option<i64>), isize>;
    type OutputStream = Stream<RootCircuit, OutputBatch>;

    #[test]
    fn mytest() {
        let mut trace = the_trace();
        trace.truncate(19);
        println!(".");
        let (circuit, (input, output, expected_output)) =
            partition_rolling_aggregate_circuit(u64::max_value(), None);

        println!("{} batches", trace.len());
        let mut n = 0;
        for mut batch in trace {
            //batch.retain(|element| element.0 > 0);
            println!("batch number {n}");
            if n == 18 {
                batch.sort_unstable();
                for item in &batch {
                    println!("{item:?}");
                }
            }
            input.append(&mut batch);
            circuit.step().unwrap();
            let out = output.consolidate();
            let exp_out = expected_output.consolidate();
            if n == 18 {
                println!("actual: {out:?}");
                println!("expected: {exp_out:?}");
            }
            if out != exp_out {
                exit(0);
            }
            n += 1;
        }
    }

    // Reference implementation of `aggregate_range` for testing.
    fn aggregate_range_slow(batch: &DataBatch, partition: u64, range: Range<u64>) -> Option<i64> {
        let mut cursor = batch.cursor();

        cursor.seek_key(&partition);
        assert!(cursor.key_valid());
        assert!(*cursor.key() == partition);
        let mut partition_cursor = PartitionCursor::new(&mut cursor);

        let mut agg = None;
        partition_cursor.seek_key(&range.from);
        while partition_cursor.key_valid() && *partition_cursor.key() <= range.to {
            while partition_cursor.val_valid() {
                let w = partition_cursor.weight() as i64;
                agg = if let Some(a) = agg {
                    Some(a + *partition_cursor.val() * w)
                } else {
                    Some(*partition_cursor.val() * w)
                };
                partition_cursor.step_val();
            }
            partition_cursor.step_key();
        }

        agg
    }

    // Reference implementation of `partitioned_rolling_aggregate` for testing.
    fn partitioned_rolling_aggregate_slow(
        stream: &DataStream,
        range_spec: RelRange<u64>,
    ) -> OutputStream {
        stream
            .gather(0)
            .integrate()
            .apply(move |batch: &DataBatch| {
                let mut tuples = Vec::with_capacity(batch.len());

                let mut cursor = batch.cursor();

                while cursor.key_valid() {
                    while cursor.val_valid() {
                        let partition = *cursor.key();
                        let (ts, _val) = *cursor.val();
                        let agg = range_spec
                            .range_of(&ts)
                            .and_then(|range| aggregate_range_slow(batch, partition, range));
                        tuples.push(((partition, (ts, agg)), 1));
                        cursor.step_val();
                    }
                    cursor.step_key();
                }

                OutputBatch::from_tuples((), tuples)
            })
            .stream_distinct()
            .gather(0)
    }

    type RangeHandle = (
        CollectionHandle<u64, ((u64, i64), isize)>,
        OutputHandle<OrdIndexedZSet<u64, (u64, Option<i64>), isize>>,
        OutputHandle<OrdIndexedZSet<u64, (u64, Option<i64>), isize>>,
    );

    fn partition_rolling_aggregate_circuit(
        lateness: u64,
        size_bound: Option<usize>,
    ) -> (CircuitHandle, RangeHandle) {
        RootCircuit::build(move |circuit| {
            let (input_stream, input_handle) =
                circuit.add_input_indexed_zset::<u64, (u64, i64), isize>();

            let aggregator = <Fold<_, DefaultSemigroup<_>, _, _>>::new(
                0i64,
                |agg: &mut i64, val: &i64, w: isize| *agg += val * (w as i64),
            );

            let range_spec = RelRange::new(RelOffset::Before(0), RelOffset::After(0));
            let expected_500_500 = partitioned_rolling_aggregate_slow(&input_stream, range_spec);
            let expected_diffs = expected_500_500.differentiate();
            let aggregate_500_500 = input_stream
                .partitioned_rolling_aggregate::<u64, i64, _>(aggregator.clone(), range_spec);
            let output_500_500 = aggregate_500_500.gather(0).integrate();
            expected_500_500.apply2(&output_500_500, |expected, actual| {
                if expected != actual {
                    println!("fail!");
                }
                //assert_eq!(expected, actual)
            });

            Ok((
                input_handle,
                aggregate_500_500.output(),
                expected_diffs.output(),
            ))
        })
        .unwrap()
    }

    use proptest::{collection, prelude::*};

    type InputTuple = (u64, ((u64, i64), isize));
    type InputBatch = Vec<InputTuple>;

    fn input_tuple(partitions: u64, window: (u64, u64)) -> impl Strategy<Value = InputTuple> {
        (
            (0..partitions),
            ((window.0..window.1, 100..101i64), 1..2isize),
        )
    }

    fn input_batch(
        partitions: u64,
        window: (u64, u64),
        max_batch_size: usize,
    ) -> impl Strategy<Value = InputBatch> {
        collection::vec(input_tuple(partitions, window), max_batch_size)
    }

    fn input_trace(
        partitions: u64,
        epoch: u64,
        max_batch_size: usize,
        max_batches: usize,
    ) -> impl Strategy<Value = Vec<InputBatch>> {
        collection::vec(
            input_batch(partitions, (0, epoch), max_batch_size),
            max_batches,
        )
    }

    static TEST_COUNT: AtomicUsize = AtomicUsize::new(0);

    proptest! {
        #[test]
        #[cfg_attr(feature = "persistence", ignore = "takes a long time?")]
        fn proptest_partitioned_over_range_dense(trace in input_trace(5, 100, 50, 20)) {
            println!(".");
            let (circuit, (input, output, expected_output)) = partition_rolling_aggregate_circuit(u64::max_value(), None);

            println!("{} batches", trace.len());
            let mut n = 0;
            let this_count = TEST_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
            if this_count == 3{
                let mut file = File::create("trace3.txt").unwrap();
                for batch in &trace {
                    writeln!(file, "vec!{batch:?},").unwrap();
                }
            };
            for mut batch in trace {
                //batch.retain(|element| element.0 > 0);
                println!("batch number {n}");
                if n == 18 {
                    batch.sort_unstable();
                    for item in &batch {
                        println!("{item:?}");
                    }
                }
                input.append(&mut batch);
                circuit.step().unwrap();
                let out = output.consolidate();
                let exp_out = expected_output.consolidate();
                if n == 18 {
                    println!("{out:?}");
                    println!("{exp_out:?}");
                }
                if out != exp_out {
                    exit(0);
                }
                n += 1;
            }
            if this_count >= 3 {
                exit(0);
            }
        }
    }
}

pub fn the_trace() -> Vec<Vec<(u64, ((u64, i64), isize))>> {
    vec![
        vec![
            (3, ((10, 100), 1)),
            (2, ((44, 100), 1)),
            (0, ((40, 100), 1)),
            (3, ((6, 100), 1)),
            (0, ((30, 100), 1)),
            (4, ((59, 100), 1)),
            (0, ((14, 100), 1)),
            (3, ((83, 100), 1)),
            (2, ((0, 100), 1)),
            (0, ((72, 100), 1)),
            (0, ((51, 100), 1)),
            (4, ((54, 100), 1)),
            (2, ((74, 100), 1)),
            (0, ((21, 100), 1)),
            (3, ((36, 100), 1)),
            (3, ((45, 100), 1)),
            (1, ((51, 100), 1)),
            (0, ((83, 100), 1)),
            (1, ((53, 100), 1)),
            (0, ((84, 100), 1)),
            (0, ((92, 100), 1)),
            (0, ((95, 100), 1)),
            (0, ((8, 100), 1)),
            (0, ((35, 100), 1)),
            (0, ((27, 100), 1)),
            (3, ((72, 100), 1)),
            (1, ((79, 100), 1)),
            (3, ((25, 100), 1)),
            (2, ((53, 100), 1)),
            (3, ((0, 100), 1)),
            (4, ((92, 100), 1)),
            (4, ((68, 100), 1)),
            (0, ((70, 100), 1)),
            (4, ((40, 100), 1)),
            (0, ((3, 100), 1)),
            (2, ((55, 100), 1)),
            (4, ((29, 100), 1)),
            (2, ((69, 100), 1)),
            (4, ((87, 100), 1)),
            (3, ((65, 100), 1)),
            (1, ((41, 100), 1)),
            (1, ((96, 100), 1)),
            (3, ((18, 100), 1)),
            (4, ((97, 100), 1)),
            (2, ((13, 100), 1)),
            (3, ((70, 100), 1)),
            (4, ((94, 100), 1)),
            (0, ((80, 100), 1)),
            (4, ((77, 100), 1)),
            (3, ((51, 100), 1)),
        ],
        vec![
            (1, ((28, 100), 1)),
            (4, ((67, 100), 1)),
            (3, ((73, 100), 1)),
            (1, ((68, 100), 1)),
            (4, ((79, 100), 1)),
            (1, ((37, 100), 1)),
            (0, ((47, 100), 1)),
            (4, ((94, 100), 1)),
            (3, ((66, 100), 1)),
            (4, ((25, 100), 1)),
            (4, ((87, 100), 1)),
            (2, ((11, 100), 1)),
            (4, ((4, 100), 1)),
            (3, ((20, 100), 1)),
            (3, ((0, 100), 1)),
            (1, ((95, 100), 1)),
            (2, ((75, 100), 1)),
            (4, ((44, 100), 1)),
            (0, ((6, 100), 1)),
            (2, ((34, 100), 1)),
            (0, ((81, 100), 1)),
            (3, ((97, 100), 1)),
            (4, ((7, 100), 1)),
            (3, ((30, 100), 1)),
            (1, ((14, 100), 1)),
            (4, ((27, 100), 1)),
            (0, ((89, 100), 1)),
            (2, ((74, 100), 1)),
            (2, ((11, 100), 1)),
            (2, ((22, 100), 1)),
            (0, ((46, 100), 1)),
            (4, ((4, 100), 1)),
            (4, ((74, 100), 1)),
            (1, ((83, 100), 1)),
            (4, ((62, 100), 1)),
            (1, ((92, 100), 1)),
            (3, ((60, 100), 1)),
            (4, ((30, 100), 1)),
            (2, ((30, 100), 1)),
            (4, ((36, 100), 1)),
            (0, ((14, 100), 1)),
            (2, ((78, 100), 1)),
            (3, ((35, 100), 1)),
            (3, ((61, 100), 1)),
            (3, ((97, 100), 1)),
            (2, ((15, 100), 1)),
            (0, ((55, 100), 1)),
            (0, ((2, 100), 1)),
            (4, ((49, 100), 1)),
            (4, ((22, 100), 1)),
        ],
        vec![
            (1, ((71, 100), 1)),
            (1, ((99, 100), 1)),
            (4, ((84, 100), 1)),
            (1, ((91, 100), 1)),
            (3, ((76, 100), 1)),
            (1, ((28, 100), 1)),
            (1, ((96, 100), 1)),
            (3, ((15, 100), 1)),
            (0, ((56, 100), 1)),
            (3, ((52, 100), 1)),
            (0, ((87, 100), 1)),
            (4, ((76, 100), 1)),
            (4, ((7, 100), 1)),
            (3, ((88, 100), 1)),
            (1, ((22, 100), 1)),
            (3, ((21, 100), 1)),
            (0, ((44, 100), 1)),
            (3, ((67, 100), 1)),
            (4, ((72, 100), 1)),
            (0, ((38, 100), 1)),
            (4, ((3, 100), 1)),
            (0, ((77, 100), 1)),
            (1, ((57, 100), 1)),
            (0, ((77, 100), 1)),
            (0, ((24, 100), 1)),
            (4, ((9, 100), 1)),
            (1, ((91, 100), 1)),
            (1, ((32, 100), 1)),
            (4, ((3, 100), 1)),
            (1, ((90, 100), 1)),
            (0, ((12, 100), 1)),
            (2, ((86, 100), 1)),
            (3, ((1, 100), 1)),
            (1, ((19, 100), 1)),
            (3, ((84, 100), 1)),
            (4, ((80, 100), 1)),
            (3, ((33, 100), 1)),
            (1, ((75, 100), 1)),
            (3, ((28, 100), 1)),
            (4, ((58, 100), 1)),
            (4, ((63, 100), 1)),
            (1, ((56, 100), 1)),
            (0, ((80, 100), 1)),
            (4, ((21, 100), 1)),
            (2, ((63, 100), 1)),
            (1, ((76, 100), 1)),
            (4, ((90, 100), 1)),
            (3, ((24, 100), 1)),
            (4, ((71, 100), 1)),
            (3, ((77, 100), 1)),
        ],
        vec![
            (2, ((83, 100), 1)),
            (3, ((90, 100), 1)),
            (3, ((24, 100), 1)),
            (4, ((58, 100), 1)),
            (0, ((60, 100), 1)),
            (2, ((28, 100), 1)),
            (3, ((97, 100), 1)),
            (0, ((11, 100), 1)),
            (0, ((36, 100), 1)),
            (3, ((50, 100), 1)),
            (1, ((19, 100), 1)),
            (2, ((96, 100), 1)),
            (0, ((97, 100), 1)),
            (2, ((58, 100), 1)),
            (0, ((0, 100), 1)),
            (4, ((94, 100), 1)),
            (4, ((37, 100), 1)),
            (2, ((44, 100), 1)),
            (1, ((13, 100), 1)),
            (1, ((79, 100), 1)),
            (4, ((33, 100), 1)),
            (1, ((84, 100), 1)),
            (1, ((37, 100), 1)),
            (3, ((80, 100), 1)),
            (3, ((46, 100), 1)),
            (3, ((21, 100), 1)),
            (0, ((79, 100), 1)),
            (3, ((51, 100), 1)),
            (1, ((8, 100), 1)),
            (3, ((87, 100), 1)),
            (3, ((86, 100), 1)),
            (3, ((51, 100), 1)),
            (4, ((33, 100), 1)),
            (3, ((1, 100), 1)),
            (4, ((54, 100), 1)),
            (2, ((68, 100), 1)),
            (4, ((11, 100), 1)),
            (4, ((10, 100), 1)),
            (1, ((20, 100), 1)),
            (2, ((14, 100), 1)),
            (2, ((29, 100), 1)),
            (3, ((99, 100), 1)),
            (4, ((31, 100), 1)),
            (1, ((89, 100), 1)),
            (4, ((95, 100), 1)),
            (4, ((66, 100), 1)),
            (1, ((14, 100), 1)),
            (0, ((82, 100), 1)),
            (2, ((59, 100), 1)),
            (3, ((10, 100), 1)),
        ],
        vec![
            (2, ((99, 100), 1)),
            (0, ((45, 100), 1)),
            (0, ((17, 100), 1)),
            (1, ((90, 100), 1)),
            (4, ((98, 100), 1)),
            (0, ((18, 100), 1)),
            (4, ((80, 100), 1)),
            (4, ((14, 100), 1)),
            (1, ((9, 100), 1)),
            (2, ((36, 100), 1)),
            (4, ((13, 100), 1)),
            (2, ((97, 100), 1)),
            (4, ((80, 100), 1)),
            (0, ((57, 100), 1)),
            (4, ((22, 100), 1)),
            (1, ((89, 100), 1)),
            (2, ((2, 100), 1)),
            (0, ((90, 100), 1)),
            (3, ((96, 100), 1)),
            (4, ((56, 100), 1)),
            (4, ((13, 100), 1)),
            (4, ((82, 100), 1)),
            (1, ((17, 100), 1)),
            (0, ((3, 100), 1)),
            (0, ((99, 100), 1)),
            (4, ((1, 100), 1)),
            (3, ((9, 100), 1)),
            (1, ((18, 100), 1)),
            (2, ((75, 100), 1)),
            (0, ((70, 100), 1)),
            (3, ((79, 100), 1)),
            (2, ((99, 100), 1)),
            (2, ((8, 100), 1)),
            (1, ((41, 100), 1)),
            (3, ((52, 100), 1)),
            (1, ((9, 100), 1)),
            (0, ((73, 100), 1)),
            (3, ((46, 100), 1)),
            (4, ((53, 100), 1)),
            (0, ((79, 100), 1)),
            (4, ((2, 100), 1)),
            (4, ((60, 100), 1)),
            (2, ((30, 100), 1)),
            (3, ((4, 100), 1)),
            (1, ((89, 100), 1)),
            (0, ((46, 100), 1)),
            (0, ((83, 100), 1)),
            (4, ((37, 100), 1)),
            (1, ((50, 100), 1)),
            (2, ((44, 100), 1)),
        ],
        vec![
            (1, ((18, 100), 1)),
            (2, ((32, 100), 1)),
            (4, ((5, 100), 1)),
            (2, ((31, 100), 1)),
            (1, ((22, 100), 1)),
            (3, ((66, 100), 1)),
            (2, ((82, 100), 1)),
            (3, ((62, 100), 1)),
            (1, ((62, 100), 1)),
            (3, ((33, 100), 1)),
            (3, ((47, 100), 1)),
            (1, ((81, 100), 1)),
            (3, ((88, 100), 1)),
            (4, ((56, 100), 1)),
            (1, ((45, 100), 1)),
            (0, ((93, 100), 1)),
            (0, ((23, 100), 1)),
            (0, ((36, 100), 1)),
            (2, ((29, 100), 1)),
            (3, ((21, 100), 1)),
            (2, ((56, 100), 1)),
            (3, ((64, 100), 1)),
            (2, ((63, 100), 1)),
            (1, ((38, 100), 1)),
            (2, ((20, 100), 1)),
            (2, ((77, 100), 1)),
            (0, ((74, 100), 1)),
            (3, ((90, 100), 1)),
            (2, ((77, 100), 1)),
            (0, ((17, 100), 1)),
            (0, ((4, 100), 1)),
            (0, ((62, 100), 1)),
            (0, ((90, 100), 1)),
            (4, ((64, 100), 1)),
            (0, ((46, 100), 1)),
            (2, ((60, 100), 1)),
            (2, ((9, 100), 1)),
            (3, ((70, 100), 1)),
            (1, ((35, 100), 1)),
            (4, ((60, 100), 1)),
            (3, ((68, 100), 1)),
            (4, ((40, 100), 1)),
            (1, ((13, 100), 1)),
            (2, ((47, 100), 1)),
            (3, ((62, 100), 1)),
            (3, ((32, 100), 1)),
            (2, ((47, 100), 1)),
            (0, ((60, 100), 1)),
            (1, ((51, 100), 1)),
            (2, ((9, 100), 1)),
        ],
        vec![
            (2, ((64, 100), 1)),
            (0, ((37, 100), 1)),
            (0, ((17, 100), 1)),
            (0, ((15, 100), 1)),
            (4, ((50, 100), 1)),
            (3, ((37, 100), 1)),
            (4, ((78, 100), 1)),
            (4, ((0, 100), 1)),
            (1, ((74, 100), 1)),
            (3, ((35, 100), 1)),
            (0, ((45, 100), 1)),
            (4, ((90, 100), 1)),
            (3, ((11, 100), 1)),
            (1, ((56, 100), 1)),
            (3, ((89, 100), 1)),
            (2, ((38, 100), 1)),
            (2, ((99, 100), 1)),
            (0, ((96, 100), 1)),
            (0, ((23, 100), 1)),
            (0, ((0, 100), 1)),
            (3, ((88, 100), 1)),
            (4, ((71, 100), 1)),
            (2, ((80, 100), 1)),
            (2, ((7, 100), 1)),
            (0, ((68, 100), 1)),
            (3, ((0, 100), 1)),
            (3, ((51, 100), 1)),
            (3, ((57, 100), 1)),
            (3, ((8, 100), 1)),
            (2, ((54, 100), 1)),
            (1, ((37, 100), 1)),
            (0, ((69, 100), 1)),
            (0, ((99, 100), 1)),
            (0, ((16, 100), 1)),
            (2, ((75, 100), 1)),
            (3, ((59, 100), 1)),
            (4, ((66, 100), 1)),
            (0, ((59, 100), 1)),
            (0, ((38, 100), 1)),
            (3, ((35, 100), 1)),
            (2, ((20, 100), 1)),
            (1, ((19, 100), 1)),
            (1, ((88, 100), 1)),
            (0, ((49, 100), 1)),
            (2, ((9, 100), 1)),
            (2, ((76, 100), 1)),
            (1, ((91, 100), 1)),
            (4, ((75, 100), 1)),
            (4, ((56, 100), 1)),
            (3, ((35, 100), 1)),
        ],
        vec![
            (3, ((94, 100), 1)),
            (4, ((93, 100), 1)),
            (0, ((57, 100), 1)),
            (0, ((2, 100), 1)),
            (0, ((25, 100), 1)),
            (1, ((62, 100), 1)),
            (3, ((71, 100), 1)),
            (2, ((38, 100), 1)),
            (1, ((18, 100), 1)),
            (1, ((50, 100), 1)),
            (4, ((79, 100), 1)),
            (0, ((57, 100), 1)),
            (3, ((89, 100), 1)),
            (0, ((18, 100), 1)),
            (3, ((78, 100), 1)),
            (1, ((88, 100), 1)),
            (1, ((76, 100), 1)),
            (0, ((21, 100), 1)),
            (4, ((16, 100), 1)),
            (2, ((85, 100), 1)),
            (0, ((55, 100), 1)),
            (0, ((68, 100), 1)),
            (2, ((43, 100), 1)),
            (3, ((38, 100), 1)),
            (2, ((22, 100), 1)),
            (1, ((89, 100), 1)),
            (1, ((10, 100), 1)),
            (0, ((99, 100), 1)),
            (2, ((40, 100), 1)),
            (1, ((53, 100), 1)),
            (1, ((32, 100), 1)),
            (1, ((99, 100), 1)),
            (3, ((36, 100), 1)),
            (1, ((93, 100), 1)),
            (4, ((48, 100), 1)),
            (2, ((6, 100), 1)),
            (3, ((28, 100), 1)),
            (2, ((89, 100), 1)),
            (2, ((14, 100), 1)),
            (3, ((60, 100), 1)),
            (3, ((21, 100), 1)),
            (1, ((25, 100), 1)),
            (2, ((43, 100), 1)),
            (1, ((37, 100), 1)),
            (1, ((86, 100), 1)),
            (1, ((30, 100), 1)),
            (1, ((86, 100), 1)),
            (2, ((71, 100), 1)),
            (0, ((25, 100), 1)),
            (1, ((7, 100), 1)),
        ],
        vec![
            (3, ((94, 100), 1)),
            (3, ((80, 100), 1)),
            (3, ((62, 100), 1)),
            (3, ((53, 100), 1)),
            (3, ((30, 100), 1)),
            (3, ((19, 100), 1)),
            (3, ((16, 100), 1)),
            (4, ((17, 100), 1)),
            (2, ((36, 100), 1)),
            (0, ((86, 100), 1)),
            (0, ((79, 100), 1)),
            (4, ((10, 100), 1)),
            (1, ((95, 100), 1)),
            (0, ((62, 100), 1)),
            (2, ((64, 100), 1)),
            (2, ((70, 100), 1)),
            (3, ((35, 100), 1)),
            (1, ((43, 100), 1)),
            (3, ((33, 100), 1)),
            (2, ((13, 100), 1)),
            (3, ((58, 100), 1)),
            (4, ((89, 100), 1)),
            (2, ((27, 100), 1)),
            (0, ((54, 100), 1)),
            (3, ((67, 100), 1)),
            (1, ((35, 100), 1)),
            (4, ((7, 100), 1)),
            (0, ((29, 100), 1)),
            (1, ((87, 100), 1)),
            (3, ((94, 100), 1)),
            (4, ((95, 100), 1)),
            (4, ((5, 100), 1)),
            (4, ((51, 100), 1)),
            (2, ((48, 100), 1)),
            (0, ((13, 100), 1)),
            (2, ((76, 100), 1)),
            (2, ((99, 100), 1)),
            (3, ((93, 100), 1)),
            (0, ((34, 100), 1)),
            (0, ((6, 100), 1)),
            (0, ((56, 100), 1)),
            (4, ((10, 100), 1)),
            (3, ((6, 100), 1)),
            (0, ((61, 100), 1)),
            (4, ((56, 100), 1)),
            (1, ((34, 100), 1)),
            (4, ((3, 100), 1)),
            (0, ((12, 100), 1)),
            (4, ((36, 100), 1)),
            (3, ((42, 100), 1)),
        ],
        vec![
            (3, ((20, 100), 1)),
            (3, ((20, 100), 1)),
            (2, ((26, 100), 1)),
            (3, ((92, 100), 1)),
            (4, ((76, 100), 1)),
            (1, ((56, 100), 1)),
            (0, ((77, 100), 1)),
            (3, ((99, 100), 1)),
            (1, ((13, 100), 1)),
            (4, ((44, 100), 1)),
            (4, ((70, 100), 1)),
            (2, ((81, 100), 1)),
            (0, ((7, 100), 1)),
            (2, ((97, 100), 1)),
            (3, ((80, 100), 1)),
            (4, ((53, 100), 1)),
            (3, ((79, 100), 1)),
            (3, ((56, 100), 1)),
            (1, ((39, 100), 1)),
            (0, ((24, 100), 1)),
            (2, ((22, 100), 1)),
            (3, ((8, 100), 1)),
            (0, ((16, 100), 1)),
            (4, ((81, 100), 1)),
            (2, ((17, 100), 1)),
            (3, ((87, 100), 1)),
            (1, ((91, 100), 1)),
            (1, ((19, 100), 1)),
            (0, ((43, 100), 1)),
            (3, ((45, 100), 1)),
            (1, ((74, 100), 1)),
            (3, ((23, 100), 1)),
            (1, ((82, 100), 1)),
            (3, ((40, 100), 1)),
            (2, ((68, 100), 1)),
            (1, ((31, 100), 1)),
            (0, ((14, 100), 1)),
            (2, ((50, 100), 1)),
            (2, ((30, 100), 1)),
            (4, ((16, 100), 1)),
            (4, ((20, 100), 1)),
            (1, ((77, 100), 1)),
            (1, ((57, 100), 1)),
            (0, ((13, 100), 1)),
            (2, ((74, 100), 1)),
            (0, ((47, 100), 1)),
            (0, ((22, 100), 1)),
            (2, ((98, 100), 1)),
            (1, ((44, 100), 1)),
            (4, ((65, 100), 1)),
        ],
        vec![
            (3, ((24, 100), 1)),
            (3, ((68, 100), 1)),
            (1, ((12, 100), 1)),
            (0, ((16, 100), 1)),
            (2, ((93, 100), 1)),
            (3, ((59, 100), 1)),
            (3, ((6, 100), 1)),
            (1, ((90, 100), 1)),
            (3, ((0, 100), 1)),
            (3, ((76, 100), 1)),
            (1, ((99, 100), 1)),
            (4, ((83, 100), 1)),
            (4, ((93, 100), 1)),
            (4, ((12, 100), 1)),
            (3, ((39, 100), 1)),
            (1, ((13, 100), 1)),
            (0, ((38, 100), 1)),
            (4, ((72, 100), 1)),
            (2, ((54, 100), 1)),
            (2, ((87, 100), 1)),
            (3, ((89, 100), 1)),
            (2, ((64, 100), 1)),
            (3, ((96, 100), 1)),
            (2, ((40, 100), 1)),
            (0, ((29, 100), 1)),
            (4, ((58, 100), 1)),
            (3, ((67, 100), 1)),
            (3, ((4, 100), 1)),
            (1, ((18, 100), 1)),
            (2, ((29, 100), 1)),
            (4, ((59, 100), 1)),
            (1, ((25, 100), 1)),
            (3, ((11, 100), 1)),
            (3, ((72, 100), 1)),
            (2, ((94, 100), 1)),
            (0, ((46, 100), 1)),
            (0, ((98, 100), 1)),
            (1, ((70, 100), 1)),
            (3, ((34, 100), 1)),
            (3, ((11, 100), 1)),
            (1, ((6, 100), 1)),
            (1, ((27, 100), 1)),
            (1, ((6, 100), 1)),
            (4, ((2, 100), 1)),
            (2, ((59, 100), 1)),
            (1, ((51, 100), 1)),
            (4, ((50, 100), 1)),
            (0, ((82, 100), 1)),
            (2, ((27, 100), 1)),
            (1, ((24, 100), 1)),
        ],
        vec![
            (2, ((46, 100), 1)),
            (0, ((51, 100), 1)),
            (4, ((75, 100), 1)),
            (4, ((62, 100), 1)),
            (3, ((52, 100), 1)),
            (3, ((91, 100), 1)),
            (3, ((85, 100), 1)),
            (4, ((26, 100), 1)),
            (2, ((0, 100), 1)),
            (4, ((44, 100), 1)),
            (0, ((14, 100), 1)),
            (4, ((0, 100), 1)),
            (0, ((50, 100), 1)),
            (4, ((93, 100), 1)),
            (2, ((94, 100), 1)),
            (4, ((60, 100), 1)),
            (0, ((97, 100), 1)),
            (0, ((34, 100), 1)),
            (2, ((88, 100), 1)),
            (4, ((83, 100), 1)),
            (1, ((68, 100), 1)),
            (1, ((82, 100), 1)),
            (3, ((82, 100), 1)),
            (1, ((27, 100), 1)),
            (2, ((32, 100), 1)),
            (1, ((59, 100), 1)),
            (3, ((42, 100), 1)),
            (1, ((8, 100), 1)),
            (1, ((81, 100), 1)),
            (1, ((22, 100), 1)),
            (2, ((71, 100), 1)),
            (1, ((70, 100), 1)),
            (2, ((22, 100), 1)),
            (2, ((80, 100), 1)),
            (3, ((22, 100), 1)),
            (2, ((77, 100), 1)),
            (2, ((24, 100), 1)),
            (4, ((21, 100), 1)),
            (0, ((12, 100), 1)),
            (3, ((42, 100), 1)),
            (3, ((88, 100), 1)),
            (0, ((67, 100), 1)),
            (2, ((16, 100), 1)),
            (0, ((24, 100), 1)),
            (4, ((7, 100), 1)),
            (0, ((44, 100), 1)),
            (1, ((61, 100), 1)),
            (3, ((98, 100), 1)),
            (3, ((72, 100), 1)),
            (2, ((84, 100), 1)),
        ],
        vec![
            (0, ((66, 100), 1)),
            (0, ((36, 100), 1)),
            (0, ((54, 100), 1)),
            (2, ((31, 100), 1)),
            (0, ((35, 100), 1)),
            (0, ((89, 100), 1)),
            (3, ((77, 100), 1)),
            (3, ((14, 100), 1)),
            (4, ((99, 100), 1)),
            (2, ((92, 100), 1)),
            (2, ((93, 100), 1)),
            (3, ((36, 100), 1)),
            (2, ((53, 100), 1)),
            (4, ((64, 100), 1)),
            (2, ((20, 100), 1)),
            (1, ((48, 100), 1)),
            (2, ((31, 100), 1)),
            (0, ((36, 100), 1)),
            (1, ((43, 100), 1)),
            (0, ((72, 100), 1)),
            (1, ((79, 100), 1)),
            (0, ((21, 100), 1)),
            (0, ((2, 100), 1)),
            (3, ((2, 100), 1)),
            (3, ((2, 100), 1)),
            (2, ((53, 100), 1)),
            (3, ((88, 100), 1)),
            (0, ((2, 100), 1)),
            (1, ((30, 100), 1)),
            (2, ((90, 100), 1)),
            (1, ((34, 100), 1)),
            (2, ((84, 100), 1)),
            (4, ((21, 100), 1)),
            (1, ((16, 100), 1)),
            (2, ((4, 100), 1)),
            (1, ((61, 100), 1)),
            (2, ((21, 100), 1)),
            (1, ((55, 100), 1)),
            (4, ((21, 100), 1)),
            (1, ((58, 100), 1)),
            (2, ((37, 100), 1)),
            (4, ((60, 100), 1)),
            (4, ((66, 100), 1)),
            (0, ((52, 100), 1)),
            (4, ((44, 100), 1)),
            (0, ((43, 100), 1)),
            (4, ((10, 100), 1)),
            (1, ((77, 100), 1)),
            (2, ((54, 100), 1)),
            (3, ((97, 100), 1)),
        ],
        vec![
            (3, ((34, 100), 1)),
            (3, ((53, 100), 1)),
            (2, ((38, 100), 1)),
            (2, ((83, 100), 1)),
            (1, ((18, 100), 1)),
            (0, ((96, 100), 1)),
            (3, ((73, 100), 1)),
            (3, ((22, 100), 1)),
            (2, ((83, 100), 1)),
            (4, ((58, 100), 1)),
            (1, ((95, 100), 1)),
            (0, ((21, 100), 1)),
            (4, ((24, 100), 1)),
            (4, ((6, 100), 1)),
            (1, ((94, 100), 1)),
            (1, ((25, 100), 1)),
            (0, ((0, 100), 1)),
            (4, ((5, 100), 1)),
            (4, ((26, 100), 1)),
            (1, ((64, 100), 1)),
            (3, ((59, 100), 1)),
            (2, ((89, 100), 1)),
            (3, ((60, 100), 1)),
            (4, ((63, 100), 1)),
            (4, ((68, 100), 1)),
            (0, ((36, 100), 1)),
            (4, ((44, 100), 1)),
            (3, ((3, 100), 1)),
            (2, ((50, 100), 1)),
            (0, ((31, 100), 1)),
            (1, ((71, 100), 1)),
            (3, ((90, 100), 1)),
            (2, ((78, 100), 1)),
            (3, ((86, 100), 1)),
            (2, ((9, 100), 1)),
            (2, ((16, 100), 1)),
            (0, ((62, 100), 1)),
            (2, ((14, 100), 1)),
            (2, ((32, 100), 1)),
            (1, ((52, 100), 1)),
            (2, ((16, 100), 1)),
            (0, ((41, 100), 1)),
            (2, ((62, 100), 1)),
            (3, ((45, 100), 1)),
            (2, ((71, 100), 1)),
            (0, ((57, 100), 1)),
            (2, ((23, 100), 1)),
            (4, ((89, 100), 1)),
            (2, ((73, 100), 1)),
            (1, ((30, 100), 1)),
        ],
        vec![
            (0, ((25, 100), 1)),
            (3, ((3, 100), 1)),
            (2, ((86, 100), 1)),
            (0, ((14, 100), 1)),
            (3, ((67, 100), 1)),
            (1, ((94, 100), 1)),
            (4, ((40, 100), 1)),
            (4, ((92, 100), 1)),
            (4, ((68, 100), 1)),
            (1, ((83, 100), 1)),
            (4, ((43, 100), 1)),
            (0, ((1, 100), 1)),
            (0, ((48, 100), 1)),
            (3, ((49, 100), 1)),
            (3, ((12, 100), 1)),
            (0, ((16, 100), 1)),
            (0, ((7, 100), 1)),
            (2, ((72, 100), 1)),
            (4, ((2, 100), 1)),
            (1, ((49, 100), 1)),
            (3, ((68, 100), 1)),
            (2, ((60, 100), 1)),
            (2, ((50, 100), 1)),
            (3, ((57, 100), 1)),
            (4, ((25, 100), 1)),
            (0, ((88, 100), 1)),
            (0, ((37, 100), 1)),
            (4, ((85, 100), 1)),
            (2, ((42, 100), 1)),
            (0, ((90, 100), 1)),
            (2, ((18, 100), 1)),
            (1, ((14, 100), 1)),
            (0, ((44, 100), 1)),
            (3, ((35, 100), 1)),
            (2, ((49, 100), 1)),
            (2, ((68, 100), 1)),
            (0, ((84, 100), 1)),
            (1, ((4, 100), 1)),
            (4, ((54, 100), 1)),
            (0, ((71, 100), 1)),
            (4, ((97, 100), 1)),
            (4, ((85, 100), 1)),
            (1, ((78, 100), 1)),
            (3, ((44, 100), 1)),
            (2, ((99, 100), 1)),
            (1, ((35, 100), 1)),
            (3, ((96, 100), 1)),
            (4, ((38, 100), 1)),
            (4, ((75, 100), 1)),
            (1, ((92, 100), 1)),
        ],
        vec![
            (1, ((27, 100), 1)),
            (1, ((11, 100), 1)),
            (1, ((49, 100), 1)),
            (4, ((21, 100), 1)),
            (2, ((26, 100), 1)),
            (2, ((10, 100), 1)),
            (3, ((21, 100), 1)),
            (1, ((63, 100), 1)),
            (2, ((66, 100), 1)),
            (0, ((72, 100), 1)),
            (3, ((32, 100), 1)),
            (0, ((41, 100), 1)),
            (3, ((23, 100), 1)),
            (4, ((85, 100), 1)),
            (2, ((0, 100), 1)),
            (0, ((10, 100), 1)),
            (2, ((90, 100), 1)),
            (2, ((14, 100), 1)),
            (4, ((19, 100), 1)),
            (0, ((99, 100), 1)),
            (4, ((48, 100), 1)),
            (4, ((17, 100), 1)),
            (0, ((24, 100), 1)),
            (1, ((46, 100), 1)),
            (3, ((75, 100), 1)),
            (2, ((13, 100), 1)),
            (2, ((72, 100), 1)),
            (0, ((37, 100), 1)),
            (2, ((61, 100), 1)),
            (1, ((44, 100), 1)),
            (2, ((47, 100), 1)),
            (0, ((73, 100), 1)),
            (2, ((59, 100), 1)),
            (3, ((81, 100), 1)),
            (4, ((81, 100), 1)),
            (2, ((29, 100), 1)),
            (3, ((59, 100), 1)),
            (4, ((32, 100), 1)),
            (1, ((70, 100), 1)),
            (1, ((88, 100), 1)),
            (4, ((13, 100), 1)),
            (1, ((65, 100), 1)),
            (0, ((82, 100), 1)),
            (1, ((44, 100), 1)),
            (4, ((41, 100), 1)),
            (2, ((83, 100), 1)),
            (2, ((51, 100), 1)),
            (3, ((70, 100), 1)),
            (1, ((8, 100), 1)),
            (3, ((70, 100), 1)),
        ],
        vec![
            (1, ((6, 100), 1)),
            (3, ((45, 100), 1)),
            (4, ((29, 100), 1)),
            (3, ((59, 100), 1)),
            (0, ((14, 100), 1)),
            (3, ((24, 100), 1)),
            (0, ((91, 100), 1)),
            (4, ((5, 100), 1)),
            (1, ((85, 100), 1)),
            (2, ((52, 100), 1)),
            (3, ((67, 100), 1)),
            (0, ((54, 100), 1)),
            (2, ((67, 100), 1)),
            (2, ((10, 100), 1)),
            (4, ((19, 100), 1)),
            (0, ((0, 100), 1)),
            (1, ((35, 100), 1)),
            (2, ((58, 100), 1)),
            (0, ((52, 100), 1)),
            (4, ((34, 100), 1)),
            (0, ((99, 100), 1)),
            (3, ((10, 100), 1)),
            (4, ((25, 100), 1)),
            (1, ((16, 100), 1)),
            (1, ((85, 100), 1)),
            (2, ((47, 100), 1)),
            (3, ((84, 100), 1)),
            (4, ((31, 100), 1)),
            (2, ((63, 100), 1)),
            (1, ((44, 100), 1)),
            (1, ((4, 100), 1)),
            (4, ((79, 100), 1)),
            (1, ((51, 100), 1)),
            (2, ((17, 100), 1)),
            (2, ((38, 100), 1)),
            (2, ((58, 100), 1)),
            (2, ((12, 100), 1)),
            (2, ((97, 100), 1)),
            (1, ((34, 100), 1)),
            (1, ((38, 100), 1)),
            (1, ((52, 100), 1)),
            (3, ((94, 100), 1)),
            (1, ((49, 100), 1)),
            (3, ((72, 100), 1)),
            (0, ((52, 100), 1)),
            (1, ((46, 100), 1)),
            (0, ((35, 100), 1)),
            (2, ((83, 100), 1)),
            (3, ((10, 100), 1)),
            (4, ((7, 100), 1)),
        ],
        vec![
            (1, ((75, 100), 1)),
            (4, ((57, 100), 1)),
            (3, ((88, 100), 1)),
            (1, ((9, 100), 1)),
            (1, ((40, 100), 1)),
            (3, ((10, 100), 1)),
            (4, ((42, 100), 1)),
            (0, ((10, 100), 1)),
            (4, ((75, 100), 1)),
            (1, ((32, 100), 1)),
            (1, ((59, 100), 1)),
            (1, ((9, 100), 1)),
            (2, ((39, 100), 1)),
            (1, ((8, 100), 1)),
            (3, ((32, 100), 1)),
            (2, ((66, 100), 1)),
            (4, ((38, 100), 1)),
            (4, ((37, 100), 1)),
            (1, ((62, 100), 1)),
            (3, ((57, 100), 1)),
            (0, ((36, 100), 1)),
            (3, ((82, 100), 1)),
            (1, ((14, 100), 1)),
            (0, ((92, 100), 1)),
            (3, ((89, 100), 1)),
            (2, ((25, 100), 1)),
            (3, ((70, 100), 1)),
            (0, ((29, 100), 1)),
            (2, ((41, 100), 1)),
            (3, ((25, 100), 1)),
            (2, ((40, 100), 1)),
            (2, ((33, 100), 1)),
            (3, ((21, 100), 1)),
            (4, ((10, 100), 1)),
            (3, ((0, 100), 1)),
            (1, ((94, 100), 1)),
            (2, ((84, 100), 1)),
            (3, ((66, 100), 1)),
            (3, ((6, 100), 1)),
            (2, ((68, 100), 1)),
            (3, ((5, 100), 1)),
            (2, ((15, 100), 1)),
            (4, ((84, 100), 1)),
            (4, ((86, 100), 1)),
            (1, ((51, 100), 1)),
            (4, ((63, 100), 1)),
            (3, ((74, 100), 1)),
            (2, ((64, 100), 1)),
            (1, ((60, 100), 1)),
            (4, ((57, 100), 1)),
        ],
        vec![
            (3, ((54, 100), 1)),
            (4, ((20, 100), 1)),
            (1, ((89, 100), 1)),
            (3, ((23, 100), 1)),
            (0, ((75, 100), 1)),
            (1, ((89, 100), 1)),
            (2, ((45, 100), 1)),
            (1, ((51, 100), 1)),
            (0, ((74, 100), 1)),
            (1, ((75, 100), 1)),
            (2, ((37, 100), 1)),
            (4, ((84, 100), 1)),
            (3, ((10, 100), 1)),
            (4, ((40, 100), 1)),
            (1, ((47, 100), 1)),
            (3, ((19, 100), 1)),
            (1, ((42, 100), 1)),
            (0, ((29, 100), 1)),
            (2, ((27, 100), 1)),
            (0, ((66, 100), 1)),
            (1, ((63, 100), 1)),
            (3, ((83, 100), 1)),
            (3, ((90, 100), 1)),
            (4, ((85, 100), 1)),
            (1, ((58, 100), 1)),
            (3, ((4, 100), 1)),
            (2, ((26, 100), 1)),
            (2, ((66, 100), 1)),
            (3, ((72, 100), 1)),
            (1, ((38, 100), 1)),
            (0, ((32, 100), 1)),
            (4, ((15, 100), 1)),
            (3, ((3, 100), 1)),
            (4, ((15, 100), 1)),
            (3, ((87, 100), 1)),
            (3, ((55, 100), 1)),
            (1, ((31, 100), 1)),
            (0, ((63, 100), 1)),
            (2, ((16, 100), 1)),
            (3, ((22, 100), 1)),
            (3, ((8, 100), 1)),
            (0, ((0, 100), 1)),
            (0, ((57, 100), 1)),
            (1, ((42, 100), 1)),
            (4, ((13, 100), 1)),
            (0, ((14, 100), 1)),
            (4, ((3, 100), 1)),
            (0, ((17, 100), 1)),
            (1, ((32, 100), 1)),
            (0, ((52, 100), 1)),
        ],
        vec![
            (2, ((59, 100), 1)),
            (4, ((71, 100), 1)),
            (0, ((8, 100), 1)),
            (4, ((63, 100), 1)),
            (3, ((31, 100), 1)),
            (3, ((37, 100), 1)),
            (0, ((92, 100), 1)),
            (2, ((74, 100), 1)),
            (2, ((30, 100), 1)),
            (1, ((7, 100), 1)),
            (3, ((40, 100), 1)),
            (4, ((71, 100), 1)),
            (1, ((98, 100), 1)),
            (2, ((78, 100), 1)),
            (0, ((58, 100), 1)),
            (1, ((46, 100), 1)),
            (4, ((7, 100), 1)),
            (2, ((33, 100), 1)),
            (0, ((43, 100), 1)),
            (3, ((36, 100), 1)),
            (1, ((48, 100), 1)),
            (4, ((72, 100), 1)),
            (4, ((15, 100), 1)),
            (4, ((19, 100), 1)),
            (1, ((91, 100), 1)),
            (1, ((94, 100), 1)),
            (2, ((66, 100), 1)),
            (0, ((63, 100), 1)),
            (4, ((10, 100), 1)),
            (3, ((1, 100), 1)),
            (0, ((8, 100), 1)),
            (3, ((28, 100), 1)),
            (1, ((3, 100), 1)),
            (2, ((23, 100), 1)),
            (0, ((55, 100), 1)),
            (1, ((16, 100), 1)),
            (2, ((35, 100), 1)),
            (3, ((53, 100), 1)),
            (0, ((39, 100), 1)),
            (1, ((33, 100), 1)),
            (3, ((45, 100), 1)),
            (4, ((27, 100), 1)),
            (1, ((0, 100), 1)),
            (4, ((81, 100), 1)),
            (4, ((66, 100), 1)),
            (1, ((27, 100), 1)),
            (3, ((56, 100), 1)),
            (2, ((79, 100), 1)),
            (3, ((73, 100), 1)),
            (1, ((36, 100), 1)),
        ],
    ]
}
