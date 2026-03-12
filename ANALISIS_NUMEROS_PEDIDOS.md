# Análisis de cambios en numeración de pedidos (app_a y app_i)

Este documento resume **todas las rutas detectadas en código** que pueden alterar el número visible de pedidos/casos en flujo.

## Conclusión corta

Tu hipótesis es **casi correcta**, pero hay matices:

1. **Sí:** un número puede cambiar al limpiar completados/cancelados (`Completados_Limpiado = sí`) porque esos registros salen del flujo y la secuencia se recalcula.
2. **Sí:** en devoluciones/casos foráneos de `app_a`, el `Numero_Foraneo` se asigna **manual** con botón y se guarda en hoja.
3. **También cambia** cuando cambia el **conjunto u orden de filas** del flujo (nuevos pedidos, reorden por fecha/sort, cambiar `Tipo_Envio`/`Tipo_Envio_Original` entre local/foráneo, cancelar/reactivar), porque la numeración se recalcula desde DataFrame en cada carga.

## app_a: cómo se calcula y cuándo cambia

- `build_flow_number_maps(...)` arma mapas de flujo con:
  - Locales: `101+`.
  - Foráneos: `01+`.
- Excluye registros completados/cancelados con `Completados_Limpiado = sí`.
- En foráneo, respeta primero los manuales (`Numero_Foraneo`) de casos/devoluciones y luego numera pedidos normales saltando números ya usados por manuales.

### Cambio manual explícito de número foráneo (devoluciones/casos)

En devoluciones foráneas existe botón **"Asignar número foráneo"**. Este:

- Busca el máximo actual en `flow_number_map_foraneo`.
- Calcula el siguiente (`max+1`).
- Escribe en Google Sheets la columna `Numero_Foraneo` del caso.

=> Este es un flujo manual y explícito, no automático por background.

## app_i: cómo se calcula y cuándo cambia

- `assign_flow_numbers(...)` calcula números para entradas locales y foráneas en memoria (display).
- También excluye cancelados y limpiados para foráneo.
- Casos/devoluciones foráneos solo muestran número si ya existe `Numero_Foraneo`; si no, se deja vacío para ese caso.
- Esta app no encontró lógica que escriba `Numero_Foraneo` ni botón para asignarlo en hoja; consume datos y los presenta.

## Sobre “debe ser manual y no automático”

Con el código actual:

- La **asignación de `Numero_Foraneo` de devoluciones** sí está implementada como manual en `app_a` (botón).
- La **renumeración de flujo** (display) sí ocurre automáticamente al recalcular mapas cuando cambia el dataset visible (limpieza, cancelación, nuevos registros, cambios de clasificación de envío, etc.).

Si tu regla de negocio es “nunca mover números ya mostrados salvo limpieza y asignación manual”, entonces hoy todavía hay recalculo automático dependiente del orden/contenido del flujo.
