SELECT
    Documento,
    Nombres_Apellidos,
    Usuairo_RR
FROM
    tb_headcount
where
    estado in ('Activo');