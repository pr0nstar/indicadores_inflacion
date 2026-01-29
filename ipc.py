#!/usr/bin/env python3

import pandas as pd
import numpy as np
import datetime as dt
import requests
from bs4 import BeautifulSoup
from supabase import create_client
import os
from time import sleep
import argparse

SB_URL = os.environ["SUPABASE_URL"]
SB_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]


def descargar_excel(url, link_texto, filename):
    def encontrar_link(url, link_texto):
        r = requests.get(url)
        html = BeautifulSoup(r.text, "html.parser")
        return [a for a in html.select("#main a") if a.get_text() == link_texto][0][
            "href"
        ]

    link = encontrar_link(url, link_texto)
    with open(filename, "wb") as f:
        f.write(requests.get(link).content)


def extraer_indice(xl, sheet_query, name):
    meses = {
        "Enero": 1,
        "Febrero": 2,
        "Marzo": 3,
        "Abril": 4,
        "Mayo": 5,
        "Junio": 6,
        "Julio": 7,
        "Agosto": 8,
        "Septiembre": 9,
        "Octubre": 10,
        "Noviembre": 11,
        "Diciembre": 12,
    }

    # Select and read the sheet
    sheet = [s for s in xl.sheet_names if sheet_query in s.lower()][0]
    df = pd.read_excel(xl, sheet, skiprows=4)

    # Select only rows with month names
    df = df[df.MES.isin(meses.keys())]

    # Simplify the table
    df = df.set_index("MES").stack().reset_index()
    df.columns = ["mes", "año", name]

    # Format dates
    df["fecha"] = pd.to_datetime(
        df.apply(lambda _: dt.date(int(_["año"]), meses[_["mes"]], 1), axis=1)
    )
    df = df[["fecha", name]]

    # Filter and type valid indice values
    df = df[~df[name].apply(lambda _: type(_) == str)]
    df[name] = df[name].astype(float)

    return df.set_index("fecha")[name].sort_index()


def extract_ciudad(xl, ciudad):
    # Read the sheet
    df = pd.read_excel(xl, ciudad, skiprows=4)

    # Select valid rows
    df = df[df["CÓDIGO"].fillna("").str.strip().str.isdigit()]

    # Simplify the table
    df = df.set_index(["CÓDIGO", "DESCRIPCIÓN"]).stack().reset_index()
    df.columns = ["codigo", "producto", "fecha", "indice"]

    # Type each column right
    df.fecha = pd.to_datetime(df.fecha)
    df.indice = df.indice.astype(float)
    df.insert(0, "ciudad", ciudad)
    df.codigo = df.codigo.str.strip()

    return df


def extraer_nacional_division(xl, sheet_query, name):
    def drop_footer_rows(raw, min_non_nan=2):
        non_nan = raw.notna().sum(axis=1)
        skip = 0
        for v in reversed(non_nan.tolist()):
            if v <= min_non_nan:
                skip += 1
            else:
                break
        return raw.iloc[:-skip] if skip > 0 else raw

    meses = [
        "ENERO",
        "FEBRERO",
        "MARZO",
        "ABRIL",
        "MAYO",
        "JUNIO",
        "JULIO",
        "AGOSTO",
        "SEPTIEMBRE",
        "OCTUBRE",
        "NOVIEMBRE",
        "DICIEMBRE",
    ]
    meses_map = {mes: i + 1 for i, mes in enumerate(meses)}
    sheet = [s for s in xl.sheet_names if sheet_query in s.lower()][0]
    raw = pd.read_excel(xl, sheet, skiprows=4, header=None)
    raw = drop_footer_rows(raw, min_non_nan=2)

    with pd.option_context("future.no_silent_downcasting", True):
        years = raw.iloc[0, 2:].ffill().infer_objects(copy=False)
    months = raw.iloc[1, 2:]

    data = raw.iloc[3:].copy()
    data.columns = ["categoria_codigo", "categoria"] + list(range(2, raw.shape[1]))

    date_cols = pd.MultiIndex.from_arrays([years, months], names=["year", "month"])
    table = data.iloc[:, 2:]
    table.columns = date_cols
    table.index = pd.MultiIndex.from_frame(data.iloc[:, :2])

    vertical = (
        table.stack([0, 1], future_stack=True)
        .reset_index(name=name)
        .dropna(subset=["year", "month", name])
    )

    vertical["year"] = vertical["year"].astype(int)
    vertical["month"] = vertical["month"].astype(str).str.strip().map(meses_map)
    vertical.insert(
        0,
        "fecha",
        vertical[["year", "month"]].apply(
            lambda _: f"{_['year']}-{_['month']}-1", axis=1
        ),
    )
    vertical.fecha = pd.to_datetime(vertical.fecha)
    vertical = vertical[["fecha", "categoria_codigo", "categoria", name]]
    vertical = vertical[vertical.categoria_codigo != 0]
    vertical[name] = vertical[name].astype(float)
    return vertical.set_index(["fecha", "categoria_codigo", "categoria"])


def indice_nacional_division(desde):
    fn = "indice.xlsx"
    descargar_excel(
        "https://www.ine.gob.bo/index.php/serie-historica-empalmada/",
        "Índice General, Variación Mensual, Acumulada y a 12 Meses por División",
        fn,
    )
    try:
        xl = pd.ExcelFile(fn)
        df = pd.concat(
            [
                extraer_nacional_division(xl, sheet, name)
                for sheet, name in zip(
                    ["ndice", "var mensual", "var acumulada", "12 meses"],
                    [
                        "indice_mensual",
                        "variacion_mensual",
                        "variacion_acumulada",
                        "variacion_12_meses",
                    ],
                )
            ],
            axis=1,
        ).reset_index()
        df = df[df.fecha.dt.year >= desde].copy()
        print(
            f"nacional por categorias: {df.shape[0]} filas {'desde ' + str(desde) if desde > 0 else ''}"
        )
        return df
    finally:
        if os.path.exists(fn):
            os.remove(fn)


def indice_nacional(desde):
    fn = "indice.xlsx"
    descargar_excel(
        "https://www.ine.gob.bo/index.php/serie-historica-empalmada/",
        "Índice General, Variación Mensual, Acumulada y a 12 Meses",
        fn,
    )
    try:
        xl = pd.ExcelFile(fn)
        df = pd.concat(
            [
                extraer_indice(xl, sheet, name)
                for sheet, name in zip(
                    ["ndice mensual", "var mensual", "var acumulada", "12 meses"],
                    [
                        "indice_mensual",
                        "variacion_mensual",
                        "variacion_acumulada",
                        "variacion_12_meses",
                    ],
                )
            ],
            axis=1,
        ).reset_index()
        df = df[df.fecha.dt.year >= desde].copy()
        print(
            f"nacional: {df.shape[0]} filas {'desde ' + str(desde) if desde > 0 else ''}"
        )
        return df
    finally:
        if os.path.exists(fn):
            os.remove(fn)


def indice_producto_ciudad():
    fn = "indice_producto_ciudad.xlsx"
    descargar_excel(
        "https://www.ine.gob.bo/index.php/ciudades-y-conurbaciones/",
        "Índices a nivel producto",
        fn,
    )
    try:
        xl = pd.ExcelFile(fn)
        ciudades = [s for s in xl.sheet_names if s != "Inicio"]
        df = pd.concat([extract_ciudad(xl, ciudad) for ciudad in ciudades])
        print(f"producto_ciudad: {df.shape[0]} filas")
        return df
    finally:
        if os.path.exists(fn):
            os.remove(fn)


def indice_producto_nacional():
    fn = "indice_producto_ciudad.xlsx"
    descargar_excel(
        "https://www.ine.gob.bo/index.php/nacional/",
        "Índice a nivel Productos",
        fn,
    )
    try:
        xl = pd.ExcelFile(fn)
        df = extract_ciudad(xl, "Bolivia")
        df.drop(columns=["ciudad"], inplace=True)
        df = df[["fecha", "codigo", "producto", "indice"]]
        print(f"producto_nacional: {df.shape[0]} filas")
        return df
    finally:
        if os.path.exists(fn):
            os.remove(fn)


def guardar(df, tabla, unique, upload=False):
    print(f"guardar {tabla}")
    df.to_csv(f"{tabla}.csv", index=False)
    df.to_excel(f"{tabla}.xlsx", index=False)
    if upload:
        chunk_size = 5000
        sleep_s = 0.2
        supabase = create_client(SB_URL, SB_KEY)
        n = len(df)
        df.fecha = df.fecha.dt.strftime("%Y-%m-%d")
        for i in range(0, n, chunk_size):
            print(f"{n if i + chunk_size > n else i + chunk_size} filas")
            chunk = df.iloc[i : i + chunk_size].copy()
            chunk = chunk.replace([np.inf, -np.inf], None)
            chunk = chunk.astype(object).where(pd.notna(chunk), None)
            supabase.table(tabla).upsert(
                chunk.to_dict(orient="records"),
                on_conflict=",".join(unique),
                ignore_duplicates=True,
            ).execute()
            sleep(sleep_s)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Descarga y guarda datos del IPC")
    parser.add_argument("--upload", action="store_true", help="Save to Supabase")
    parser.add_argument("--desde", type=int, help="Desde qué año", default=0)
    args = parser.parse_args()

    producto_nacional = indice_producto_nacional()
    guardar(
        producto_nacional,
        "ine_ipc_producto_nacional",
        ["fecha", "codigo"],
        upload=args.upload,
    )

    nacional_division = indice_nacional_division(args.desde)
    guardar(
        nacional_division,
        "ine_ipc_nacional_divisiones",
        ["fecha", "categoria_codigo"],
        upload=args.upload,
    )

    nacional = indice_nacional(args.desde)
    guardar(nacional, "ine_ipc_nacional", ["fecha"], upload=args.upload)

    producto_ciudad = indice_producto_ciudad()
    guardar(
        producto_ciudad,
        "ine_ipc_producto_ciudad",
        ["fecha", "ciudad", "codigo"],
        upload=args.upload,
    )
