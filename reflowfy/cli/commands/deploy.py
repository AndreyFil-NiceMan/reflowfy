"""Deploy Reflowfy to Kubernetes/OpenShift using Helm."""

import os
import subprocess
import typer
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import reflowfy
from reflowfy.cli.utils import console, get_helm_chart_path


def register(app: typer.Typer):
    """Register the deploy command."""

    @app.command()
    def deploy(
        registry: Optional[str] = typer.Option(None, envvar="REGISTRY", help="Registry where images are stored (or set REGISTRY in .env)"),
        kafka: Optional[str] = typer.Option(None, envvar="KAFKA_BOOTSTRAP_SERVERS", help="External Kafka Broker (or set KAFKA_BOOTSTRAP_SERVERS in .env)"),
        namespace: str = typer.Option(None, envvar="NAMESPACE", help="Kubernetes namespace (or set NAMESPACE in .env)"),
        deploy_postgres: bool = typer.Option(True, envvar="DEPLOY_POSTGRES", help="Deploy PostgreSQL (set to False to use external DB)"),
        postgres_image_repo: Optional[str] = typer.Option(None, envvar="POSTGRES_IMAGE_REPOSITORY", help="Custom PostgreSQL image repository"),
        postgres_image_tag: Optional[str] = typer.Option(None, envvar="POSTGRES_IMAGE_TAG", help="Custom PostgreSQL image tag"),
        keda: bool = typer.Option(False, "--keda/--no-keda", help="Enable KEDA autoscaling for workers"),
        keda_min: int = typer.Option(0, "--keda-min", help="KEDA minimum replicas"),
        keda_max: int = typer.Option(100, "--keda-max", help="KEDA maximum replicas"),
        kafka_topic: Optional[str] = typer.Option(None, "--kafka-topic", envvar="KAFKA_TOPIC", help="Kafka topic name (or set KAFKA_TOPIC in .env)"),
        workers: int = typer.Option(1, "--workers", help="Worker replicas (when KEDA disabled)"),
    ):
        """
        Deploy Reflowfy to Kubernetes/OpenShift using Helm.

        Reads configuration from .env file if present. Command-line options override .env values.
        """
        from rich.panel import Panel

        namespace = namespace or "reflowfy"
        kafka_topic = kafka_topic or "reflow.jobs"
        
        if not registry:
            console.print("❌ Registry is required. Set --registry or REGISTRY in .env", style="red")
            if not Path(".env").exists():
                 console.print("⚠️  No .env file found in current directory.", style="yellow")
            raise typer.Exit(code=1)
        
        if not kafka:
            console.print("❌ Kafka is required. Set --kafka or KAFKA_BOOTSTRAP_SERVERS in .env", style="red")
            raise typer.Exit(code=1)
        
        console.print(Panel(f"🚀 Deploying Reflowfy to namespace: [bold cyan]{namespace}[/bold cyan]"))

        try:
            chart_path = get_helm_chart_path()
            console.print(f"📦 Using Helm chart at: {chart_path}", style="dim")
        except FileNotFoundError as e:
            console.print(f"❌ {e}", style="red")
            raise typer.Exit(code=1)

        # Construct Helm command
        cmd = [
            "helm", "upgrade", "--install", "reflowfy", str(chart_path),
            "--namespace", namespace,
            "--set", f"api.image.repository={registry}/{namespace}/reflowfy-api",
            "--set", f"reflowManager.image.repository={registry}/{namespace}/reflowfy-reflow-manager",
            "--set", f"worker.image.repository={registry}/{namespace}/reflowfy-worker",
            "--set", f"api.image.tag={reflowfy.__version__}",
            "--set", f"reflowManager.image.tag={reflowfy.__version__}",
            "--set", f"worker.image.tag={reflowfy.__version__}",
            "--set", "api.image.pullPolicy=Always",
            "--set", "reflowManager.image.pullPolicy=Always",
            "--set", "worker.image.pullPolicy=Always",
            "--set", f"kafka.external.bootstrapServers={kafka.replace(',', '\\,')}",
            "--set", f"kafka.topic={kafka_topic}",
            "--set", "api.service.type=ClusterIP",
            "--set", "reflowManager.service.type=ClusterIP",
        ]
        
        # PostgreSQL configuration
        if deploy_postgres:
            cmd.extend(["--set", "postgresql.enabled=true"])
            if postgres_image_repo:
                cmd.extend(["--set", f"postgresql.image.repository={postgres_image_repo}"])
                # If a custom repository is provided, clear the registry to prevent prepending specific defaults
                cmd.extend(["--set", "postgresql.image.registry="])
                # Enable insecure images to bypass Bitnami's check for unrecognized images
                cmd.extend(["--set", "global.security.allowInsecureImages=true"])
            if postgres_image_tag:
                 cmd.extend(["--set", f"postgresql.image.tag={postgres_image_tag}"])
        else:
            db_url = os.getenv("DATABASE_URL")
            if not db_url:
                 console.print("❌ DATABASE_URL is required when DEPLOY_POSTGRES is false.", style="red")
                 raise typer.Exit(code=1)
            
            try:
                url = urlparse(db_url)
                console.print(f"🔗 Configuring external PostgreSQL: {url.hostname}:{url.port or 5432}", style="blue")
                cmd.extend([
                    "--set", "postgresql.enabled=false",
                    "--set", f"postgresql.external.host={url.hostname}",
                    "--set", f"postgresql.external.port={url.port or 5432}",
                    "--set", f"postgresql.external.database={url.path.lstrip('/')}",
                    "--set", f"postgresql.external.username={url.username}",
                    "--set", f"postgresql.external.password={url.password or ''}",
                ])
            except Exception as e:
                console.print(f"❌ Failed to parse DATABASE_URL: {e}", style="red")
                raise typer.Exit(code=1)
        
        # KEDA configuration
        if keda:
            cmd.extend([
                "--set", "worker.keda.enabled=true",
                "--set", f"worker.keda.minReplicaCount={keda_min}",
                "--set", f"worker.keda.maxReplicaCount={keda_max}",
            ])
        else:
            cmd.extend([
                "--set", "worker.keda.enabled=false",
                "--set", f"worker.replicaCount={workers}",
            ])

        console.print("\n📋 [bold]Helm Command:[/bold]")
        console.print(" \\\n  ".join(cmd), style="cyan")
        
        console.print(f"\n🔧 Running Helm upgrade...", style="yellow")
        try:
            subprocess.run(cmd, check=True)
            console.print("✅ Helm upgrade successful", style="green")
        except subprocess.CalledProcessError:
            console.print("❌ Deployment failed", style="red")
            raise typer.Exit(code=1)

        # OpenShift Routes
        console.print("\n🌐 Creating OpenShift Routes...", style="blue")
        try:
            subprocess.run(
                ["oc", "expose", "svc/reflowfy-api", "--name=reflowfy-api", "-n", namespace],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            subprocess.run(
                ["oc", "expose", "svc/reflowfy-reflow-manager", "--name=reflowfy-manager", "-n", namespace],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            console.print("✅ Routes created/verified", style="green")
        except FileNotFoundError:
            console.print("⚠️ 'oc' command not found. Skipping route creation.", style="yellow")

        console.print(Panel(f"""
🎉 Deployment Complete!

📋 Check status:
   oc get pods -n {namespace}
   oc get routes -n {namespace}

📊 KEDA status (if enabled):
   oc get scaledobject -n {namespace}
""", style="bold green"))
