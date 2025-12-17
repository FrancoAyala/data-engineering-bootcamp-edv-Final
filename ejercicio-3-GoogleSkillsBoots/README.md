# Ejercicio 3 – Google Cloud Dataprep (Google Skills Boost)

Este ejercicio corresponde a un **laboratorio guiado de Google Skills Boost** donde se construye un pipeline de transformación de datos usando:

- **BigQuery** como origen y destino de datos.
- **Cloud Dataprep / Alteryx Designer Cloud** para exploración y limpieza.
- **Dataflow** como motor de ejecución de los jobs de Dataprep.

El dataset es de **e‑commerce (Google Merchandise Store)**, trabajando sobre una muestra de ~56k sesiones de Google Analytics.

---



## 1. Laboratorio realizado (resumen del flujo)

1. **Preparación en BigQuery**
   - Creación del dataset `ecommerce`.
   - Creación de la tabla `ecommerce.all_sessions_raw_dataprep` a partir del dataset público `data-to-insights.ecommerce.all_sessions_raw` filtrando la fecha `20170801`.

2. **Conexión desde Dataprep**
   - Apertura de Dataprep desde la consola de GCP.
   - Creación del *flow* **“Ecommerce Analytics Pipeline”**.
   - Importación de la tabla `all_sessions_raw_dataprep` desde BigQuery como dataset de entrada.
  
3. **Exploración inicial**
   - Revisión de calidad de columnas con los histogramas de Dataprep.
   - Identificación de columnas clave: `channelGrouping`, `country`, `totalTransactionRevenue`, `timeOnSite`, `pageviews`, `sessionQualityDim`, etc.
   - Detección de campos con **muchos nulos** (por ejemplo `totalTransactionRevenue`) y columnas con **tipo mal detectado** (por ejemplo `productSKU`).

4. **Limpieza de datos**
   - Forzar tipo **STRING** para `productSKU`.
   - Eliminación de columnas que no aportan al análisis: `itemQuantity`, `itemRevenue` (todas nulas).
   - Eliminación de filas duplicadas usando la funcionalidad de *Remove duplicate rows*.
   - Filtro de sesiones sin revenue: se conservan solo las filas con `totalTransactionRevenue` no nulo.
   - Filtro de filas donde `type = "PAGE"` para evitar doble conteo con otros tipos de hits.

5. **Enriquecimiento**
   - Creación de un **ID de sesión único**:
     - Columna `unique_session_id` = `fullVisitorId` + `"-"` + `visitId`.
   - Creación de columna categórica `eCommerceAction_label` con un **CASE** sobre `eCommerceAction_type`:
     - Ejemplos: `0 = 'Unknown'`, `3 = 'Add product(s) to cart'`, `6 = 'Completed purchase'`, etc.
   - Ajuste de `totalTransactionRevenue`:
     - Nueva columna `totalTransactionRevenue1 = totalTransactionRevenue / 1_000_000`.
     - Cambio de tipo a **Decimal** para poder usarla como métrica de negocio.

6. **Ejecución del job**
   - Ejecución del *job* de Dataprep con entorno **Dataflow + BigQuery**.
   - Publicación del resultado en BigQuery:
     - Dataset: `ecommerce`.
     - Tabla destino: `revenue_reporting`.
     - Opción: *Drop the table every run* para recrear la tabla en cada corrida del pipeline.


---

## 2. Preguntas técnicas (resumen)

> Todas las respuestas se basan en la muestra cargada en Dataprep.
- **Cantidad de columnas del dataset:** 32 columnas.  
- **Cantidad de filas en la muestra de Dataprep:** ~12.000 filas (subset de las ~56k reales).  
- **Valor más frecuente en `channelGrouping`:** `Referral`.  
- **Top 3 países por cantidad de sesiones:** `United States`, `India`, `United Kingdom`.  
- **Barra gris bajo `totalTransactionRevenue`:** indica **valores faltantes** (sesiones sin revenue).  
- **Máximos en el sample:**
  - `timeOnSite`: 5.561 segundos aprox.
  - `pageviews`: 155 páginas.
  - `sessionQualityDim`: ~97.
- **Distribución de `sessionQualityDim`:** muy sesgada a valores bajos (mayoría de sesiones de baja calidad).  
- **Rango de fechas del dataset:** solo **1 día**, `2017‑08‑01`.  
- **Barra roja en `productSKU`:** valores **mismatch** porque Dataprep lo infiere como numérico, pero realmente debe ser **STRING** (hay SKUs alfanuméricos).  
- **Productos más populares (`v2ProductName`):** línea de productos **Nest**.  
- **Categorías más frecuentes (`v2ProductCategory`):** `Nest`, `Bags` y `(not set)`.  
- **Valor dominante en `productVariant`:** `(not set)` (la mayoría no tiene variante de color/talla).  
- **Valores posibles en `type`:** `PAGE` y `EVENT`.
  - **Máximo `productQuantity`:** ~100 unidades.  
- **Moneda dominante (`currencyCode`):** `USD`.  
- **`itemQuantity` e `itemRevenue`:** 100% nulos, no se usan.  
- **% de `transactionId` válidos:** ~4,6% → representa la **tasa de conversión** aproximada.  
- **Cantidad de valores en `eCommerceAction_type`:** 7 valores distintos en la muestra; el más común es `0` (acción desconocida).  
- **`eCommerceAction_type = 6` significa:** `Completed purchase` (compra completada).

---

## 3. Conclusiones (enfoque Data Engineering)

- El dataset original está **en bruto**: muchas columnas con nulos, tipos erróneos y campos duplicados.
- Cloud Dataprep permite **industrializar la limpieza**: tipado correcto, eliminación de columnas, filtros y creación de columnas derivadas sin escribir código SQL/Spark.
- La tabla final `revenue_reporting`:
  - Contiene **solo sesiones con revenue**.
  - Tiene un **ID de sesión único** (`unique_session_id`).
  - Incluye métricas listas para reporting: `totalTransactionRevenue1`, `pageviews`, `sessionQualityDim`, etc.
- Desde la óptica de ingeniería de datos, el flujo implementa claramente las capas:
  - **Raw**: tabla `all_sessions_raw_dataprep`.
  - **Staging / limpieza**: pasos en Dataprep.
  - **Curated / analytics**: tabla `revenue_reporting` en BigQuery.
 
Recomendaciones:

- Particionar la tabla final por fecha para mejorar performance de consultas.
- Documentar el pipeline en un *data catalog* (por ejemplo, **Data Catalog de GCP**).
- Programar las ejecuciones de Dataprep/Dataflow mediante **Cloud Composer (Airflow managed)** para que la actualización sea automática.

---

## 4. Arquitectura propuesta en Google Cloud

Arquitectura alternativa (orientada a GCP) para este caso de uso:

1. **Ingesta y almacenamiento**
   - **Cloud Storage **.
   - Carga inicial a **BigQuery** usando **Dataflow** o **BigQuery Data Transfer**.

2. **Transformaciones**
   - Pipeline de limpieza y enriquecimiento con:
     - **Dataprep** 
     - **Dataflow / Dataproc (Spark)**
    
3. **Orquestación**
   - **Cloud Composer (Airflow)** para coordinar:
     - Carga raw → transformaciones Dataprep/Dataflow → publicación en BigQuery.
    
4. **Consumo analítico**
   - **BigQuery** como *data warehouse* central.
   - **Looker Studio / Looker** para dashboards de negocio
  
4. **Consumo analítico**
   - **BigQuery** como *data warehouse* central.
   - **Looker Studio / Looker** para dashboards de negocio (ingresos, conversión, productos top, etc.).







