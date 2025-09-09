import subprocess

import rich_click as click


@click.command()
@click.option(
    "--local-dask-port",
    default=8787,
    help="Local port to forward from (Dask) [default: 8787]",
)
@click.option(
    "--remote-dask-port",
    default=8787,
    help="Remote port to forward to (Dask) [default: 8787]",
)
@click.option(
    "--local-prefect-port",
    default=4200,
    help="Local port to forward from (Prefect) [default: 4200]",
)
@click.option(
    "--remote-prefect-port",
    default=4200,
    help="Remote port to forward to (Prefect) [default: 4200]",
)
@click.option(
    "--gateway",
    default="levante1.dkrz.de",
    help="Gateway server hostname [default: levante1.dkrz.de]",
)
@click.option("--compute-node", required=True, help="Compute node hostname")
@click.option("--username", required=True, help="Username for SSH connections")
def ssh_tunnel_cli(
    local_dask_port,
    remote_dask_port,
    local_prefect_port,
    remote_prefect_port,
    gateway,
    compute_node,
    username,
):
    """
    Create an SSH tunnel to access Prefect and Dask dashboards on a remote compute node.
    """
    dask_link = click.style(
        f"http://localhost:{local_dask_port}/status", fg="blue", underline=True
    )
    prefect_link = click.style(
        f"http://localhost:{local_prefect_port}", fg="blue", underline=True
    )

    ssh_command = (
        f"ssh -nNT "
        f"-L {local_dask_port}:{compute_node}:{remote_dask_port} "
        f"-L {local_prefect_port}:{compute_node}:{remote_prefect_port} "
        f"{username}@{gateway}"
    )

    click.echo(f"Creating SSH tunnel via: {ssh_command}")
    click.echo(
        f"Port forwarding: localhost:{local_dask_port} -> "
        f"{gateway}:{remote_dask_port} -> {compute_node}:{remote_dask_port}"
    )
    click.echo(
        f"Port forwarding: localhost:{local_prefect_port} -> "
        f"{gateway}:{remote_prefect_port} -> {compute_node}:{remote_prefect_port}"
    )
    click.echo(f"Dask Dashboard will be accessible at {dask_link}")
    click.echo(f"Prefect Dashboard will be accessible at {prefect_link}")
    click.echo("Press Ctrl+C to close the tunnel")

    try:
        # Run the SSH command
        subprocess.run(ssh_command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        click.echo(f"An error occurred while creating the SSH tunnel: {e}")
    except KeyboardInterrupt:
        click.echo("SSH tunnel closed.")


if __name__ == "__main__":
    ssh_tunnel_cli()
