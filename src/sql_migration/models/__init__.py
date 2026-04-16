"""
sql_migration.models — public API for all Pydantic I/O schemas.
"""
from sql_migration.models.common import (
    ChunkInfo, ColumnDiffStatus, ColumnInfo, ComplexityScore,
    ConversionStrategy, DetectionResolution, ErrorEntry, LoopGuards,
    ProcStatus, ResolutionAction, TableRegistryEntry, TableStatus,
    TodoItem, ValidationLevel, ValidationOutcome,
)
from sql_migration.models.detection import (
    AdapterRegistryDescription, AdapterSeedProcBoundary,
    AdapterSeedTransactionPatterns, ConstructMap, DialectAdapter,
    DialectProfile, ReadmeSignals,
)
from sql_migration.models.analysis import (
    AnalysisAgentInput, AnalysisAgentOutput, ComplexityEntry,
    ComplexityReport, ConversionOrder, DependencyGraph, Manifest,
    PerProcAnalysis, PipelineConfig, ProcEntry, ProcSummaryForLLM, TableRef,
    TableRegistry,
    build_llm_complexity_input, parse_llm_complexity_output,
)
from sql_migration.models.planning import (
    ChunkPlan, ModuleGroupEntry, ModuleGroupingPlan, Plan,
    PlanningAgentInput, PlanningAgentOutput, ProcChunkPlan, ProcPlan,
    ProcStrategyContext, SourceProcRange, StrategyEntry, StrategyMap,
    build_llm_module_grouping_input, build_llm_strategy_input,
    parse_llm_module_grouping_output, parse_llm_strategy_output,
)
from sql_migration.models.pipeline import (
    ChunkConversionResult, Checkpoint, ColumnDiff, ConversionLog,
    DispatchTask, ExecutionResult, FeedbackBundle, FeedbackResolution,
    ProcState, ReplanRequest, ValidationResult,
    build_llm_conversion_input, build_llm_self_correction_input,
    build_llm_semantic_diff_input, parse_column_diffs,
    split_code_and_test,
)
from sql_migration.models.semantic import (
    DataFlowEntry, DiscoveredDependency, PipelineSemanticMap, PipelineStage,
    ProcSemanticEntry, TargetArchitecture,
)