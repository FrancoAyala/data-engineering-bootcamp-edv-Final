-- =========================================================
--  CONSULTAS DE NEGOCIO - EJERCICIO 1 AVIACIÓN
--  Autor: Franco Ayala
-- =========================================================



6.Determinar la cantidad de vuelos entre las fechas 01/12/2021 y 31/01/2022. Mostrar consulta y Resultado de la query

USE dw_aviacion;

SELECT COUNT(*) AS cant_vuelos
FROM vuelos_ar_clean
WHERE fecha BETWEEN DATE '2021-12-01' AND DATE '2022-01-31';



7. Cantidad de pasajeros que viajaron en Aerolíneas Argentinas entre el 01/01/2021 y 30/06/2022. Mostrar consulta y Resultado de la query

USE dw_aviacion;

SELECT
  SUM(pasajeros) AS pasajeros_total
FROM vuelos_ar_clean
WHERE fecha BETWEEN DATE '2021-01-01' AND DATE '2022-06-30'
  AND aerolinea_nombre = 'AEROLINEAS ARGENTINAS SA';





8. Mostrar fecha, hora, código aeropuerto salida, ciudad de salida, código de aeropuerto de arribo, ciudad de arribo, y cantidad de pasajeros de cada vuelo, entre el 01/01/2022 y el 30/06/2022 ordenados por fecha de manera descendiente. Mostrar consulta y Resultado de la query

USE dw_aviacion;

WITH vuelos_mapeados AS (
  SELECT
    v.fecha,
    v.horautc,
    CASE WHEN lower(v.tipo_de_movimiento) LIKE 'despeg%' THEN v.aeropuerto
         ELSE v.origen_destino
    END AS cod_aeropuerto_salida,
    CASE WHEN lower(v.tipo_de_movimiento) LIKE 'despeg%' THEN v.origen_destino
         ELSE v.aeropuerto
    END AS cod_aeropuerto_arribo,
    v.pasajeros
  FROM vuelos_ar_clean v
  WHERE v.fecha BETWEEN DATE '2022-01-01' AND DATE '2022-06-30'
),
enriquecido AS (
  SELECT
    m.fecha,
    m.horautc,
    m.cod_aeropuerto_salida,
    m.cod_aeropuerto_arribo,
    m.pasajeros
  FROM vuelos_mapeados m
  LEFT JOIN aeropuertos_ar_clean a_s
    ON upper(trim(a_s.iata)) = upper(trim(m.cod_aeropuerto_salida))
  LEFT JOIN aeropuertos_ar_clean a_r
    ON upper(trim(a_r.iata)) = upper(trim(m.cod_aeropuerto_arribo))
)
SELECT
  fecha,
  horautc,
  cod_aeropuerto_salida,
  cod_aeropuerto_arribo,
  pasajeros
FROM enriquecido
ORDER BY fecha DESC, horautc DESC;






9. Cuales son las 10 aerolíneas que más pasajeros llevaron entre el 01/01/2021 y el 30/06/2022 exceptuando aquellas aerolíneas que no tengan nombre. Mostrar consulta y Visualización

USE dw_aviacion;

-- Top 10 aerolíneas por cantidad de pasajeros (2021-01-01 a 2022-06-30)
SELECT
  UPPER(TRIM(aerolinea_nombre)) AS aerolinea,
  SUM(COALESCE(pasajeros,0))     AS total_pasajeros
FROM vuelos_ar_clean
WHERE fecha BETWEEN DATE '2021-01-01' AND DATE '2022-06-30'
  -- excluir sin nombre
  AND aerolinea_nombre IS NOT NULL
  AND TRIM(aerolinea_nombre) NOT IN ('', '0', 'N/A', 'NULL')
GROUP BY UPPER(TRIM(aerolinea_nombre))
ORDER BY total_pasajeros DESC
LIMIT 10;



