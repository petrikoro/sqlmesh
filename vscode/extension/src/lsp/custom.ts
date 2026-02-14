export interface AllModelsMethod {
  method: 'sqlmesh/all_models'
  request: AllModelsRequest
  response: AllModelsResponse
}

export interface RenderModelMethod {
  method: 'sqlmesh/render_model'
  request: RenderModelRequest
  response: RenderModelResponse
}

interface RenderModelRequest {
  textDocumentUri: string
}

interface RenderModelResponse extends BaseResponse {
  models: RenderModelEntry[]
}

export interface RenderModelEntry {
  name: string
  fqn: string
  description: string | null | undefined
  rendered_query: string
}

export type CustomLSPMethods =
  | AllModelsMethod
  | RenderModelMethod
  | AllModelsForRenderMethod
  | SupportedMethodsMethod
  | FormatProjectMethod
  | ListWorkspaceTests
  | ListDocumentTests
  | RunTest
  | GetEnvironmentsMethod
  | GetTableDiffModelsMethod
  | GetApiModelsMethod
  | GetModelLineageMethod
  | GetColumnLineageMethod
  | GetTableDiffMethod

interface AllModelsRequest {
  textDocument: {
    uri: string
  }
}

interface AllModelsResponse extends BaseResponse {
  models: string[]
  keywords: string[]
}

export interface GetApiModelsMethod {
  method: 'sqlmesh/get_api_models'
  request: GetApiModelsRequest
  response: GetApiModelsResponse
}

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
interface GetApiModelsRequest {}

interface GetApiModelsResponse extends BaseResponse {
  data: any[]
}

export interface GetModelLineageMethod {
  method: 'sqlmesh/get_model_lineage'
  request: GetModelLineageRequest
  response: GetModelLineageResponse
}

interface GetModelLineageRequest {
  modelName: string
}

interface GetModelLineageResponse extends BaseResponse {
  data: Record<string, string[]>
}

export interface GetColumnLineageMethod {
  method: 'sqlmesh/get_column_lineage'
  request: GetColumnLineageRequest
  response: GetColumnLineageResponse
}

interface GetColumnLineageRequest {
  modelName: string
  columnName: string
  modelsOnly?: boolean
}

interface GetColumnLineageResponse extends BaseResponse {
  data: Record<string, Record<string, any>>
}

export interface GetTableDiffMethod {
  method: 'sqlmesh/get_table_diff'
  request: GetTableDiffLSPRequest
  response: GetTableDiffLSPResponse
}

interface GetTableDiffLSPRequest {
  source: string
  target: string
  on?: string
  model_or_snapshot?: string
  where?: string
  temp_schema?: string
  limit?: number
}

interface GetTableDiffLSPResponse extends BaseResponse {
  data: any | null
}

export interface AllModelsForRenderMethod {
  method: 'sqlmesh/all_models_for_render'
  request: AllModelsForRenderRequest
  response: AllModelsForRenderResponse
}

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
interface AllModelsForRenderRequest {}

interface AllModelsForRenderResponse extends BaseResponse {
  models: ModelForRendering[]
}

export interface ModelForRendering {
  name: string
  fqn: string
  description: string | null | undefined
  uri: string
}

export interface SupportedMethodsMethod {
  method: 'sqlmesh/supported_methods'
  request: SupportedMethodsRequest
  response: SupportedMethodsResponse
}

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
interface SupportedMethodsRequest {}

interface SupportedMethodsResponse extends BaseResponse {
  methods: CustomMethod[]
}

interface CustomMethod {
  name: string
}

export interface FormatProjectMethod {
  method: 'sqlmesh/format_project'
  request: FormatProjectRequest
  response: FormatProjectResponse
}

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
interface FormatProjectRequest {}

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
interface FormatProjectResponse extends BaseResponse {}

interface BaseResponse {
  response_error?: string
}

export interface ListWorkspaceTests {
  method: 'sqlmesh/list_workspace_tests'
  request: ListWorkspaceTestsRequest
  response: ListWorkspaceTestsResponse
}

type ListWorkspaceTestsRequest = object

interface Position {
  line: number
  character: number
}

interface Range {
  start: Position
  end: Position
}

interface TestEntry {
  name: string
  uri: string
  range: Range
}

interface ListWorkspaceTestsResponse extends BaseResponse {
  tests: TestEntry[]
}

export interface ListDocumentTests {
  method: 'sqlmesh/list_document_tests'
  request: ListDocumentTestsRequest
  response: ListDocumentTestsResponse
}

export interface DocumentIdentifier {
  uri: string
}

export interface ListDocumentTestsRequest {
  textDocument: DocumentIdentifier
}

export interface ListDocumentTestsResponse extends BaseResponse {
  tests: TestEntry[]
}

export interface RunTest {
  method: 'sqlmesh/run_test'
  request: RunTestRequest
  response: RunTestResponse
}

export interface RunTestRequest {
  textDocument: DocumentIdentifier
  testName: string
}

export interface RunTestResponse extends BaseResponse {
  success: boolean
  error_message?: string
}

export interface GetEnvironmentsMethod {
  method: 'sqlmesh/get_environments'
  request: GetEnvironmentsRequest
  response: GetEnvironmentsResponse
}

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
interface GetEnvironmentsRequest {}

interface GetEnvironmentsResponse extends BaseResponse {
  environments: Record<string, EnvironmentInfo>
  pinned_environments: string[]
  default_target_environment: string
}

interface EnvironmentInfo {
  name: string
  snapshots: string[]
  start_at: string
  plan_id: string
}

export interface GetTableDiffModelsMethod {
  method: 'sqlmesh/get_models'
  request: GetModelsRequest
  response: GetModelsResponse
}

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
interface GetModelsRequest {}

interface GetModelsResponse extends BaseResponse {
  models: ModelInfo[]
}

interface ModelInfo {
  name: string
  fqn: string
  description: string | null | undefined
}
