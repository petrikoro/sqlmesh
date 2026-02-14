import { type UseQueryResult, useQuery } from '@tanstack/react-query'
import { callRpc } from '@/utils/rpc'
import { isErr } from '@bus/result'
import type {
  Model,
  ModelLineageResponse,
  ColumnLineageResponse,
} from './types'

export function useApiModels(): UseQueryResult<Model[]> {
  return useQuery({
    queryKey: ['/api/models'],
    queryFn: async () => {
      const result = await callRpc('get_api_models', {})
      if (isErr(result)) throw new Error(result.error)
      return result.value.data as Model[]
    },
  })
}

export function useApiModelLineage(
  modelName: string,
): UseQueryResult<ModelLineageResponse> {
  return useQuery({
    queryKey: ['/api/lineage', modelName],
    queryFn: async () => {
      const result = await callRpc('get_model_lineage', { modelName })
      if (isErr(result)) throw new Error(result.error)
      return result.value.data
    },
  })
}

export function useApiColumnLineage(
  model: string,
  column: string,
  params?: { models_only?: boolean },
): UseQueryResult<ColumnLineageResponse> {
  return useQuery({
    queryKey: ['/api/lineage', model, column],
    queryFn: async () => {
      const result = await callRpc('get_column_lineage', {
        modelName: model,
        columnName: column,
        modelsOnly: params?.models_only,
      })
      if (isErr(result)) throw new Error(result.error)
      return result.value.data
    },
  })
}
