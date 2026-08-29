# Contexto de implementación

Este repositorio implementa una prueba de concepto académica DataOps para
"FinanData Perú". Las decisiones de arquitectura que no deben alterarse son:

- Patrón ELT con capas Bronze, Silver y Gold.
- Prefect 3 se ejecuta localmente; no se usa Prefect Cloud.
- Las fuentes ATM (CSV), banca móvil (REST simulada) y Core/ACH (CDC simulado)
  convergen en una única carga Bronze. ATM usa mapping para LIM-001, LIM-002 y
  LIM-003.
- Bronze conserva el RAW y añade metadatos de trazabilidad antes de cualquier
  validación.
- Data Contract se ejecuta antes de Data Quality. Un contrato inválido se
  conserva en `schema-quarantine/` y detiene el lote.
- Data Quality separa válidos y rechazados; los rechazados se conservan en
  `data-quarantine/`.
- Quality Gate 1 autoriza o bloquea Silver según una política externa. El valor
  de 5 % es exclusivamente un umbral configurable de demostración.
- La cadena posterior es Silver -> Gold -> MERGE/UPSERT DWH -> Post-load ->
  Reconciliación -> Quality Gate 2 -> publicaciones paralelas -> notificación.
- Las publicaciones SBS, Riesgo y BI dependen inmediatamente de QG2. La
  notificación de éxito depende de las tres publicaciones.
- Supabase Storage representa el Data Lake y Supabase PostgreSQL el DWH. Los
  backends locales permiten desarrollo y CI sin secretos.
- No hay ciclos de reprocesamiento dentro del flow. Corregir y reprocesar
  implica iniciar un flow run nuevo y parametrizado.
- Los retries se reservan para fallos técnicos transitorios: 3 en ingestas y
  persistencias; 2 en publicaciones; 0 en reglas y transformaciones.
- Observabilidad y alertas son transversales, no tasks ficticias del DAG.

Escenarios obligatorios: `success`, `schema_fail`, `incident_15_percent` y
`qg2_fail`. El incidente procesa exactamente 100 registros, rechaza 15 y queda
bloqueado en QG1 sin promover ni publicar datos.

