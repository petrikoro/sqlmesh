/**
 * Domain types for the SQLMesh API.
 *
 * These match the Pydantic models in sqlmesh/api/models.py.
 */

export type ModelType = (typeof ModelType)[keyof typeof ModelType]

// eslint-disable-next-line @typescript-eslint/no-redeclare
export const ModelType = {
  python: 'python',
  sql: 'sql',
  seed: 'seed',
  external: 'external',
  source: 'source',
} as const

export type ColumnDescription = string | null

export interface Column {
  name: string
  type: string
  description?: ColumnDescription
}

export interface Reference {
  name: string
  expression: string
  unique: boolean
}

export type IntervalUnit = (typeof IntervalUnit)[keyof typeof IntervalUnit]

// eslint-disable-next-line @typescript-eslint/no-redeclare
export const IntervalUnit = {
  year: 'year',
  month: 'month',
  day: 'day',
  hour: 'hour',
  half_hour: 'half_hour',
  quarter_hour: 'quarter_hour',
  five_minute: 'five_minute',
} as const

export interface ModelDetails {
  owner?: string | null
  kind?: string | null
  batch_size?: number | null
  cron?: string | null
  stamp?: string | number | null
  start?: string | number | null
  retention?: number | null
  table_format?: string | null
  storage_format?: string | null
  time_column?: string | null
  tags?: string | null
  references?: Reference[]
  partitioned_by?: string | null
  clustered_by?: string | null
  lookback?: number | null
  cron_prev?: string | number | null
  cron_next?: string | number | null
  interval_unit?: IntervalUnit | null
  annotated?: boolean | null
}

export interface Model {
  name: string
  fqn: string
  path?: string | null
  full_path?: string | null
  dialect: string
  type: ModelType
  columns: Column[]
  description?: string | null
  details?: ModelDetails | null
  sql?: string | null
  definition?: string | null
  default_catalog?: string | null
  hash: string
}

export interface LineageColumn {
  source?: string | null
  expression?: string | null
  models: { [key: string]: string[] }
}

export interface SchemaDiff {
  source: string
  target: string
  source_schema: { [key: string]: string }
  target_schema: { [key: string]: string }
  added: { [key: string]: string }
  removed: { [key: string]: string }
  modified: { [key: string]: string }
}

export interface RowDiff {
  source: string
  target: string
  stats: { [key: string]: number }
  sample: { [key: string]: unknown }
  joined_sample: { [key: string]: unknown }
  s_sample: { [key: string]: unknown }
  t_sample: { [key: string]: unknown }
  column_stats: { [key: string]: unknown }
  source_count: number
  target_count: number
  count_pct_change: number
  decimals: number
  processed_sample_data?: ProcessedSampleData | null
}

export interface ProcessedSampleData {
  column_differences: { [key: string]: unknown }[]
  source_only: { [key: string]: unknown }[]
  target_only: { [key: string]: unknown }[]
}

export interface TableDiff {
  schema_diff: SchemaDiff
  row_diff: RowDiff
  on: string[][]
}

/** Response shape for model lineage endpoint. */
export type ModelLineageResponse = { [key: string]: string[] }

/** Response shape for column lineage endpoint. */
export type ColumnLineageResponse = {
  [key: string]: { [key: string]: LineageColumn }
}
