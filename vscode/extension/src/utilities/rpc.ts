import type { CallbackEvent } from '@bus/callbacks'
import { isErr } from '@bus/result'

/**
 * Build an RPC response callback from an LSP result.
 *
 * This is a shared helper used by webview message handlers
 * (lineagePanel, tableDiff) to convert an LSP response into the
 * `CallbackEvent` shape expected by the React webview.
 */
export function buildRpcResponse(
  requestId: string,
  response: any,
): CallbackEvent {
  if (isErr(response)) {
    const err = response.error as {
      type?: string
      message?: string
    }
    let errorMessage: string
    switch (err.type) {
      case 'generic':
        errorMessage = err.message ?? 'Unknown error'
        break
      case 'invalid_state':
        errorMessage = `Invalid state: ${err.message ?? 'Unknown error'}`
        break
      case 'sqlmesh_outdated':
        errorMessage = `SQLMesh version issue: ${err.message ?? 'Unknown error'}`
        break
      default:
        errorMessage = 'Unknown error'
    }
    return {
      key: 'rpcResponse',
      payload: {
        requestId,
        result: {
          ok: false,
          error: errorMessage,
        },
      },
    }
  }
  return {
    key: 'rpcResponse',
    payload: {
      requestId,
      result: response,
    },
  }
}
