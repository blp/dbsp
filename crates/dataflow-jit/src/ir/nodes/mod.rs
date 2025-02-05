mod aggregate;
mod constant;
mod differentiate;
mod filter_map;
mod flat_map;
mod index;
mod io;
mod join;
mod subgraph;
mod sum;

pub use crate::ir::NodeId;
pub use aggregate::{Fold, Max, Min, PartitionedRollingFold};
pub use constant::ConstantStream;
pub use differentiate::{Differentiate, Integrate};
pub use filter_map::{Filter, FilterMap, Map};
pub use flat_map::FlatMap;
pub use index::{IndexByColumn, IndexWith, UnitMapToSet};
pub use io::{Export, ExportedNode, Sink, Source, SourceMap};
pub use join::{Antijoin, JoinCore, MonotonicJoin};
pub use subgraph::Subgraph;
pub use sum::{Minus, Sum};

use crate::ir::{function::Function, layout_cache::RowLayoutCache, LayoutId};
use derive_more::{IsVariant, Unwrap};
use enum_dispatch::enum_dispatch;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[enum_dispatch(DataflowNode)]
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, IsVariant, Unwrap)]
pub enum Node {
    Map(Map),
    Min(Min),
    Max(Max),
    Neg(Neg),
    Sum(Sum),
    Fold(Fold),
    Sink(Sink),
    Minus(Minus),
    Filter(Filter),
    FilterMap(FilterMap),
    Source(Source),
    SourceMap(SourceMap),
    IndexWith(IndexWith),
    Differentiate(Differentiate),
    Integrate(Integrate),
    Delta0(Delta0),
    DelayedFeedback(DelayedFeedback),
    Distinct(Distinct),
    JoinCore(JoinCore),
    Subgraph(Subgraph),
    Export(Export),
    ExportedNode(ExportedNode),
    MonotonicJoin(MonotonicJoin),
    ConstantStream(ConstantStream),
    PartitionedRollingFold(PartitionedRollingFold),
    FlatMap(FlatMap),
    Antijoin(Antijoin),
    IndexByColumn(IndexByColumn),
    UnitMapToSet(UnitMapToSet),
    // TODO: OrderBy, Windows
}

impl Node {
    pub const fn as_constant(&self) -> Option<&ConstantStream> {
        if let Self::ConstantStream(constant) = self {
            Some(constant)
        } else {
            None
        }
    }

    pub const fn as_antijoin(&self) -> Option<&Antijoin> {
        if let Self::Antijoin(antijoin) = self {
            Some(antijoin)
        } else {
            None
        }
    }
}

// TODO: Fully flesh this out, make it useful
#[enum_dispatch]
pub trait DataflowNode {
    fn map_inputs<F>(&self, map: &mut F)
    where
        F: FnMut(NodeId);

    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        self.map_inputs(&mut |node_id| inputs.push(node_id));
    }

    fn map_inputs_mut<F>(&mut self, map: &mut F)
    where
        F: FnMut(&mut NodeId);

    fn output_stream(&self, inputs: &[StreamLayout]) -> Option<StreamLayout>;

    fn output_kind(&self, inputs: &[StreamLayout]) -> Option<StreamKind> {
        self.output_stream(inputs).map(StreamLayout::kind)
    }

    fn validate(&self, inputs: &[StreamLayout], layout_cache: &RowLayoutCache);

    fn optimize(&mut self, layout_cache: &RowLayoutCache);

    fn functions<'a>(&'a self, _functions: &mut Vec<&'a Function>) {}

    fn functions_mut<'a>(&'a mut self, _functions: &mut Vec<&'a mut Function>) {}

    fn map_layouts<F>(&self, map: &mut F)
    where
        F: FnMut(LayoutId);

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>);
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Deserialize,
    Serialize,
    JsonSchema,
    IsVariant,
    Unwrap,
)]
pub enum StreamLayout {
    Set(LayoutId),
    Map(LayoutId, LayoutId),
}

impl StreamLayout {
    pub const fn key_layout(self) -> LayoutId {
        match self {
            Self::Set(key) | Self::Map(key, _) => key,
        }
    }

    pub const fn value_layout(self) -> Option<LayoutId> {
        match self {
            Self::Set(_) => None,
            Self::Map(_, value) => Some(value),
        }
    }

    pub const fn kind(self) -> StreamKind {
        match self {
            Self::Set(_) => StreamKind::Set,
            Self::Map(_, _) => StreamKind::Map,
        }
    }

    pub(crate) fn map_layouts<F>(self, map: &mut F)
    where
        F: FnMut(LayoutId),
    {
        match self {
            Self::Set(key) => map(key),
            Self::Map(key, value) => {
                map(key);
                map(value);
            }
        }
    }

    pub(crate) fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        match self {
            Self::Set(key) => *key = mappings[key],
            Self::Map(key, value) => {
                *key = mappings[key];
                *value = mappings[value];
            }
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Deserialize,
    Serialize,
    JsonSchema,
    IsVariant,
)]
pub enum StreamKind {
    Set,
    Map,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, JsonSchema)]
pub struct Distinct {
    input: NodeId,
    layout: StreamLayout,
}

impl Distinct {
    pub const fn new(input: NodeId, layout: StreamLayout) -> Self {
        Self { input, layout }
    }

    pub const fn input(&self) -> NodeId {
        self.input
    }

    pub const fn layout(&self) -> StreamLayout {
        self.layout
    }
}

impl DataflowNode for Distinct {
    fn map_inputs<F>(&self, map: &mut F)
    where
        F: FnMut(NodeId),
    {
        map(self.input);
    }

    fn map_inputs_mut<F>(&mut self, map: &mut F)
    where
        F: FnMut(&mut NodeId),
    {
        map(&mut self.input);
    }

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(self.layout)
    }

    fn validate(&self, inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0], self.layout);
    }

    fn optimize(&mut self, _layout_cache: &RowLayoutCache) {}

    fn map_layouts<F>(&self, map: &mut F)
    where
        F: FnMut(LayoutId),
    {
        self.layout.map_layouts(map);
    }

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        self.layout.remap_layouts(mappings);
    }
}

// FIXME: DelayedFeedback with maps
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct DelayedFeedback {
    layout: LayoutId,
}

impl DelayedFeedback {
    pub const fn new(layout: LayoutId) -> Self {
        Self { layout }
    }

    pub const fn layout(&self) -> LayoutId {
        self.layout
    }
}

impl DataflowNode for DelayedFeedback {
    fn map_inputs<F>(&self, _map: &mut F)
    where
        F: FnMut(NodeId),
    {
    }

    fn map_inputs_mut<F>(&mut self, _map: &mut F)
    where
        F: FnMut(&mut NodeId),
    {
    }

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(StreamLayout::Set(self.layout))
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn optimize(&mut self, _layout_cache: &RowLayoutCache) {}

    fn map_layouts<F>(&self, map: &mut F)
    where
        F: FnMut(LayoutId),
    {
        map(self.layout);
    }

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        self.layout = mappings[&self.layout];
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct Delta0 {
    input: NodeId,
}

impl Delta0 {
    pub const fn new(input: NodeId) -> Self {
        Self { input }
    }

    pub const fn input(&self) -> NodeId {
        self.input
    }
}

impl DataflowNode for Delta0 {
    fn map_inputs<F>(&self, map: &mut F)
    where
        F: FnMut(NodeId),
    {
        map(self.input);
    }

    fn map_inputs_mut<F>(&mut self, map: &mut F)
    where
        F: FnMut(&mut NodeId),
    {
        map(&mut self.input);
    }

    fn output_stream(&self, inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(inputs[0])
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn optimize(&mut self, _layout_cache: &RowLayoutCache) {}

    fn map_layouts<F>(&self, _map: &mut F)
    where
        F: FnMut(LayoutId),
    {
    }

    fn remap_layouts(&mut self, _mappings: &BTreeMap<LayoutId, LayoutId>) {}
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct Neg {
    input: NodeId,
    layout: StreamLayout,
}

impl Neg {
    pub fn new(input: NodeId, layout: StreamLayout) -> Self {
        Self { input, layout }
    }

    pub const fn input(&self) -> NodeId {
        self.input
    }

    pub const fn layout(&self) -> StreamLayout {
        self.layout
    }
}

impl DataflowNode for Neg {
    fn map_inputs<F>(&self, map: &mut F)
    where
        F: FnMut(NodeId),
    {
        map(self.input);
    }

    fn map_inputs_mut<F>(&mut self, map: &mut F)
    where
        F: FnMut(&mut NodeId),
    {
        map(&mut self.input);
    }

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(self.layout)
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn optimize(&mut self, _layout_cache: &RowLayoutCache) {}

    fn map_layouts<F>(&self, map: &mut F)
    where
        F: FnMut(LayoutId),
    {
        self.layout.map_layouts(map);
    }

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        self.layout.remap_layouts(mappings);
    }
}
