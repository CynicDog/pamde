"""
pamde CLI entry points.

    uv run pamde edit my_data.parquet
    uv run pamde inspect my_data.parquet
"""

from __future__ import annotations

import typer

app = typer.Typer(
    name="pamde",
    help="Parquet Metadata Editor — inspect and tag Parquet column metadata.",
)


@app.command()
def edit(
    path: str = typer.Argument(..., help="Path to the Parquet file to edit."),
    host: str = typer.Option("127.0.0.1", help="Server host."),
    port: int = typer.Option(2971, help="Server port."),
    no_browser: bool = typer.Option(False, "--no-browser", help="Don't open browser."),
) -> None:
    """Open the metadata editor UI for a Parquet file."""
    import webbrowser

    import uvicorn

    from pamde.server.app import create_app

    server_app = create_app(parquet_path=path)
    url = f"http://{host}:{port}"

    if not no_browser:
        import threading

        def _open() -> None:
            import time

            time.sleep(0.8)
            webbrowser.open(url)

        threading.Thread(target=_open, daemon=True).start()

    typer.echo(f"pamde editor running at {url}")
    typer.echo("Press Ctrl+C to stop.")
    uvicorn.run(server_app, host=host, port=port)


@app.command()
def inspect(
    path: str = typer.Argument(..., help="Path to the Parquet file."),
    json: bool = typer.Option(False, "--json", help="Output as JSON."),
) -> None:
    """Print column metadata summary to stdout."""
    import json as _json

    from pamde.editor import ParquetEditor

    editor = ParquetEditor.open(path)
    cols = editor.columns()

    if json:
        import dataclasses

        typer.echo(_json.dumps([dataclasses.asdict(c) for c in cols], indent=2))
    else:
        typer.echo(f"\nFile: {path}")
        typer.echo(f"Columns ({len(cols)}):\n")
        for col in cols:
            typer.echo(f"  {col.path_in_schema}")
            typer.echo(f"    type:        {col.physical_type} / {col.logical_type or '-'}")
            typer.echo(f"    repetition:  {col.repetition}")
            typer.echo(f"    null_count:  {col.null_count}")
            typer.echo(f"    range:       [{col.min_value}, {col.max_value}]")
            typer.echo(f"    compression: {col.compression}")
            if col.tags:
                typer.echo(f"    tags:        {col.tags}")
            typer.echo()
