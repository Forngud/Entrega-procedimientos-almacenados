CREATE OR ALTER PROCEDURE spGenerarEncuentrosSiguienteFase
    @IdCampeonato INT,
    @IdFaseOrigen INT,
    @IdFaseDestino INT,
    @IdEstadio INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        BEGIN TRAN;

        IF NOT EXISTS (SELECT 1 FROM Campeonato WHERE Id = @IdCampeonato)
        BEGIN
            RAISERROR('Campeonato no existe.',16,1);
            ROLLBACK;
            RETURN;
        END;

        IF @IdEstadio IS NULL
            SELECT TOP(1) @IdEstadio = Id FROM Estadio ORDER BY Id;

        ;WITH Resultados AS (
            SELECT 
                e.IdCampeonato,
                gp.IdGrupo,
                e.IdPais1 AS IdPais,
                e.GolesPais1 AS GF,
                e.GolesPais2 AS GC,
                CASE 
                    WHEN e.GolesPais1 > e.GolesPais2 THEN 3
                    WHEN e.GolesPais1 = e.GolesPais2 THEN 1
                    ELSE 0
                END AS Puntos
            FROM Encuentro e
            INNER JOIN GrupoPais gp ON gp.IdPais = e.IdPais1
            WHERE e.IdFase = @IdFaseOrigen AND e.IdCampeonato = @IdCampeonato
            UNION ALL
            SELECT 
                e.IdCampeonato,
                gp.IdGrupo,
                e.IdPais2 AS IdPais,
                e.GolesPais2 AS GF,
                e.GolesPais1 AS GC,
                CASE 
                    WHEN e.GolesPais2 > e.GolesPais1 THEN 3
                    WHEN e.GolesPais2 = e.GolesPais1 THEN 1
                    ELSE 0
                END AS Puntos
            FROM Encuentro e
            INNER JOIN GrupoPais gp ON gp.IdPais = e.IdPais2
            WHERE e.IdFase = @IdFaseOrigen AND e.IdCampeonato = @IdCampeonato
        ),
        Tabla AS (
            SELECT 
                IdGrupo, IdPais,
                SUM(Puntos) AS Puntos,
                SUM(GF) AS GF,
                SUM(GC) AS GC,
                SUM(GF) - SUM(GC) AS Dif
            FROM Resultados
            GROUP BY IdGrupo, IdPais
        ),
        Posiciones AS (
            SELECT 
                IdGrupo, IdPais,
                ROW_NUMBER() OVER (PARTITION BY IdGrupo ORDER BY Puntos DESC, Dif DESC, GF DESC) AS Posicion
            FROM Tabla
        ),
        Grupos AS (
            SELECT Id AS IdGrupo,
                   ROW_NUMBER() OVER (ORDER BY Id) AS NumGrupo
            FROM Grupo
            WHERE IdCampeonato = @IdCampeonato
        ),
        Cruces AS (
            SELECT g1.IdGrupo AS GrupoA, g2.IdGrupo AS GrupoB
            FROM Grupos g1
            JOIN Grupos g2 ON g2.NumGrupo = g1.NumGrupo + 1
            WHERE g1.NumGrupo % 2 = 1
        ),
        Partidos AS (
            SELECT pa1.IdPais AS IdPais1, pb2.IdPais AS IdPais2 FROM Cruces c
                JOIN Posiciones pa1 ON pa1.IdGrupo = c.GrupoA AND pa1.Posicion = 1
                JOIN Posiciones pb2 ON pb2.IdGrupo = c.GrupoB AND pb2.Posicion = 2
            UNION ALL
            SELECT pa2.IdPais, pb1.IdPais FROM Cruces c
                JOIN Posiciones pa2 ON pa2.IdGrupo = c.GrupoA AND pa2.Posicion = 2
                JOIN Posiciones pb1 ON pb1.IdGrupo = c.GrupoB AND pb1.Posicion = 1
        )
        INSERT INTO Encuentro (IdPais1, IdPais2, IdFase, IdCampeonato, IdEstadio)
        SELECT p.IdPais1, p.IdPais2, @IdFaseDestino, @IdCampeonato, @IdEstadio
        FROM Partidos p
        WHERE NOT EXISTS (
            SELECT 1
            FROM Encuentro e
            WHERE e.IdCampeonato = @IdCampeonato
              AND e.IdFase = @IdFaseDestino
              AND ((e.IdPais1 = p.IdPais1 AND e.IdPais2 = p.IdPais2)
                OR  (e.IdPais1 = p.IdPais2 AND e.IdPais2 = p.IdPais1))
        );

        COMMIT;
        PRINT 'Encuentros de la siguiente fase generados correctamente.';
    END TRY
    BEGIN CATCH
        ROLLBACK;
        DECLARE @ErrorMsg NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(@ErrorMsg,16,1);
    END CATCH
END;
GO
