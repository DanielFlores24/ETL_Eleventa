"""
Este archivo es el archivo de extraccion y transformacion de la informacion de la db.
Para este ocupa obtenerse PDVDATA.FDB
"""
import fdb
import pandas as pd
import os

#=============== Extracción =================================
def ETL(tienda):
    #Ruta absoluta al archivo .fdb
    ruta_fdb = fr'P:\tu\ruta\absoluta\alroot\{tienda}\PDVDATA.FDB'
    carpeta_salida = f"{tienda}"

    #Lista de tablas que quieres extraer
    tablas_a_extraer = [
        "VENTATICKETS_ARTICULOS",
        "VENTATICKETS",
        "PRODUCTOS",
        #"INVENTARIO_HISTORIAL",
        "DEPARTAMENTOS",
        #"CORTE_VENTAS_POR_DEPTO",
        #"CORTE_OPERACIONES",
        #"CLIENTESV2"
    ]

    #Conexión
    con = fdb.connect(
        dsn=ruta_fdb,
        user='SYSDBA',
        password='masterkey'  
    )

    #Tablas
    for tabla in tablas_a_extraer:
        print(f"Extrayendo tabla: {tabla}")
        query = f"SELECT * FROM {tabla}"
        df = pd.read_sql(query, con)
        ruta_csv = os.path.join(carpeta_salida, f"{tabla}.csv")
        df.to_csv(ruta_csv, index=False)
        print(f"Tabla {tabla} exportada a {ruta_csv}")

    con.close()

    # =============== Transformación ==================================
    #---------------- Ventas Tickets ----------------------------------
    rVentasTickets = f"{tienda}\VENTATICKETS.csv"
    VentasTickets = pd.read_csv(rVentasTickets)
    VentasTickets["TIENDA"] = tienda
    VentasTickets = VentasTickets[[
        "ID",  "NOMBRE", "TOTAL", "VENDIDO_EN", "NUMERO_ARTICULOS"
    ]]
    VentasTickets["VENDIDO_EN"] = pd.to_datetime(VentasTickets["VENDIDO_EN"])
    VentasTickets["HORA"] = VentasTickets["VENDIDO_EN"].dt.strftime("%H:%M:%S")
    VentasTickets["FECHA"] = VentasTickets["VENDIDO_EN"].dt.strftime("%d/%m/%Y")
    VentasTickets = VentasTickets.drop(columns="VENDIDO_EN")
    VentasTickets["NOMBRE"] =VentasTickets["NOMBRE"].astype(str)
    VentasTickets["NOMBRE"] = VentasTickets["NOMBRE"].str.split().str[0]
    VentasTickets["TOTAL"] =VentasTickets["TOTAL"].round().astype(int)
    VentasTickets["ID"] =VentasTickets["ID"].round().astype(int)
    VentasTickets["NUMERO_ARTICULOS"] =VentasTickets["NUMERO_ARTICULOS"].round().astype(int)
    VentasTickets = VentasTickets.rename(columns={
        "NUMERO_ARTICULOS": "NUMARTICULOS",
        "ID": "TICKET_ID"
        })
    VentasTickets = VentasTickets.dropna()
    VentasTickets.to_csv(rVentasTickets, index=False)
    #---------------- Ventas Articulos ----------------------------------
    rventasArticulos = f"{tienda}\VENTATICKETS_ARTICULOS.csv"
    ventasArticulos = pd.read_csv(rventasArticulos)
    ventasArticulos["TIENDA"] = tienda
    ventasArticulos = ventasArticulos[[
        "TICKET_ID",  "PRODUCTO_CODIGO", "PRODUCTO_NOMBRE", "CANTIDAD", "DEPARTAMENTO_ID"
    ]]
    ventasArticulos["PRODUCTO_CODIGO"] =ventasArticulos["PRODUCTO_CODIGO"].astype(str)
    ventasArticulos["TICKET_ID"] =ventasArticulos["TICKET_ID"].round().astype(int)
    ventasArticulos["PRODUCTO_NOMBRE"] =ventasArticulos["PRODUCTO_NOMBRE"].astype(str)
    ventasArticulos["CANTIDAD"] =ventasArticulos["CANTIDAD"].round().astype(int)
    ventasArticulos["DEPARTAMENTO_ID"] =ventasArticulos["DEPARTAMENTO_ID"].round().astype(int)
    ventasArticulos = ventasArticulos.rename(columns={
        "PRODUCTO_NOMBRE": "NOMBREPRODUCTO"
        })
    ventasArticulos = ventasArticulos.dropna()
    ventasArticulos.to_csv(rventasArticulos, index=False)
    #------------------- Productos ----------------------------------
    rProductos = f"{tienda}\PRODUCTOS.csv"
    Productos = pd.read_csv(rProductos)
    Productos["TIENDA"] = tienda
    Productos = Productos[[
        "CODIGO",  "DESCRIPCION", "PVENTA", "DEPT"
    ]]
    Productos["CODIGO"] =Productos["CODIGO"].astype(str)
    Productos["PVENTA"] =Productos["PVENTA"].round().astype(int)
    Productos["DESCRIPCION"] =Productos["DESCRIPCION"].astype(str)
    Productos["DEPT"] =Productos["DEPT"].round().astype(int)
    Productos = Productos.rename(columns={
        "CODIGO": "PRODUCTO_CODIGO",
        "DESCRIPCION":"NOMBREPRODUCTO",
        "PVENTA": "PRECIO",
        "DEPT": "DEPARTAMENTO_ID"
        })
    Productos = Productos.dropna()
    Productos.to_csv(rProductos, index=False)

    #------------------- Departamentos ----------------------------------
    rDepartamento = f"{tienda}\DEPARTAMENTOS.csv"
    Departamento = pd.read_csv(rDepartamento)
    Departamento["TIENDA"] = tienda
    Departamento = Departamento[Departamento["ACTIVO"]==1]
    Departamento = Departamento[[
        "ID",  "NOMBRE"
    ]]
    Departamento["NOMBRE"] =Departamento["NOMBRE"].astype(str)
    Departamento["ID"] =Departamento["ID"].round().astype(int)
    Departamento = Departamento.rename(columns={
        "ID": "DEPARTAMENTO_ID",
        "NOMBRE":"NOMBREDEPARTAMENTO"
        })
    Departamento = Departamento.dropna()
    Departamento.to_csv(rDepartamento, index=False)

# ========================= Merge ===============================
def TablaFinal():
    dfs_final = []   
    #Colocar aqui los nombres de tus carpetas de tiendas
    tiendas = ["Tienda1", "Tienda2"]

    for tienda in tiendas:
        ETL(tienda)  

        try:
            df_tickets = pd.read_csv(f"{tienda}/VENTATICKETS.csv")
            df_articulos = pd.read_csv(f"{tienda}/VENTATICKETS_ARTICULOS.csv")
            df_productos = pd.read_csv(f"{tienda}/PRODUCTOS.csv")
            df_departamentos = pd.read_csv(f"{tienda}/DEPARTAMENTOS.csv")
        except FileNotFoundError as e:
            print(f"Error: {e}")
            continue

        # MERGE de esa tienda
        df_merge = pd.merge(
            df_tickets,
            df_articulos,
            on=["TICKET_ID"],
            how="inner"
        )
        df_merge = pd.merge(
            df_merge,
            df_productos,
            on=["PRODUCTO_CODIGO", "DEPARTAMENTO_ID", "NOMBREPRODUCTO"],
            how="inner"
        )
        df_merge = pd.merge(
            df_merge,
            df_departamentos,
            on=["DEPARTAMENTO_ID"],
            how="inner"
        )

        df_merge["TIENDA"] = tienda 
        dfs_final.append(df_merge)  

        print(f"Merge de {tienda}: {len(df_merge)} filas")

    # Una sola vez: juntar todo y guardar
    df_final = pd.concat(dfs_final, ignore_index=True)
    df_final.to_csv("VENTAS_COMPLETO.csv", index=False)
    print(f"\nArchivo VENTAS_COMPLETO.csv generado con {len(df_final)} filas")

# =============== Código Final =============================
TablaFinal()

