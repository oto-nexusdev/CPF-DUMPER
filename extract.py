import os
import re
import platform
import logging
import time
import threading
import tkinter as tk
from tkinter import filedialog
from concurrent.futures import ThreadPoolExecutor
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.live import Live
from rich.table import Table

# Inicializa o console Rich
console = Console()

# Configuração de logging para registrar erros em um arquivo
logging.basicConfig(
    filename="erros.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w"
)

# Variável global para contar pares encontrados
total_pares_encontrados = {"CPF": 0, "CNPJ": 0, "Email": 0}
contador_lock = threading.Lock()  # Lock para proteger o contador global


def limpar_console():
    """Limpa o console."""
    os.system('cls' if platform.system() == 'Windows' else 'clear')


def criar_pasta_resultados():
    """
    Cria a pasta 'resultados_extracao' no diretório atual, caso ela não exista.
    """
    pasta_resultados = os.path.join(os.getcwd(), "resultados_extracao")
    if not os.path.exists(pasta_resultados):
        os.makedirs(pasta_resultados)
    return pasta_resultados


def salvar_resultados(nome_arquivo, dados, tipo_extracao):
    """
    Salva os dados extraídos em um arquivo dentro da pasta 'resultados_extracao'.
    Remove duplicatas e informa o total de duplicados removidos.
    """
    pasta_resultados = criar_pasta_resultados()
    caminho_arquivo = os.path.join(pasta_resultados, nome_arquivo)

    # Utiliza um dicionário para remover duplicatas enquanto mantém a ordem
    dados_unicos = {dado: None for dado in dados}.keys()
    duplicados = len(dados) - len(dados_unicos)

    try:
        with open(caminho_arquivo, "a", encoding="utf-8") as arquivo:
            arquivo.writelines(f"{linha}\n" for linha in dados_unicos)
        
        console.print(f"[bold green]Dados salvos com sucesso em:[/bold green] [yellow]{caminho_arquivo}[/yellow]")

        # Feedback visual sobre duplicatas
        if duplicados > 0:
            console.print(
                f"[bold yellow]{duplicados} linhas duplicadas foram detectadas e removidas. "
                f"O arquivo final contém {len(dados_unicos)} {tipo_extracao}(s).[/bold yellow]"
            )
    except Exception as e:
        console.print(f"[bold red]Erro ao salvar os dados no arquivo {nome_arquivo}: {e}[/bold red]")
        logging.error(f"Erro ao salvar os dados no arquivo {nome_arquivo}: {e}")


def gerar_nome_arquivo(nome_base):
    """
    Gera um nome de arquivo único baseado no nome base.
    Se o arquivo já existir, adiciona um sufixo numérico incremental.
    """
    pasta_resultados = criar_pasta_resultados()
    caminho_arquivo = os.path.join(pasta_resultados, nome_base)

    if not os.path.exists(caminho_arquivo):
        return caminho_arquivo
    
    base, ext = os.path.splitext(caminho_arquivo)
    contador = 1
    while os.path.exists(f"{base}_{contador}{ext}"):
        contador += 1
    return f"{base}_{contador}{ext}"


def processar_arquivo(pasta, nome_arquivo, padrao, nome_arquivo_saida, progresso_total, lock, progress, tipo_extracao):
    """
    Processa um único arquivo, extraindo dados com base no padrão fornecido.
    Atualiza a barra de progresso e o contador global de pares.
    """
    global total_pares_encontrados

    pares = []
    caminho_arquivo = os.path.join(pasta, nome_arquivo)

    try:
        with open(caminho_arquivo, 'r', encoding='utf-8', errors='ignore') as arquivo:
            for linha in arquivo:
                pares.extend(re.findall(padrao, linha))
    except Exception as e:
        console.print(f"[red]Erro ao processar o arquivo {nome_arquivo}: {e}[/red]")
        logging.error(f"Erro ao processar o arquivo {nome_arquivo}: {e}")

    if pares:
        salvar_resultados(nome_arquivo_saida, pares, tipo_extracao)

        with contador_lock:
            total_pares_encontrados[tipo_extracao] += len(pares)

    # Atualiza a barra de progresso
    with lock:
        progress.update(progresso_total, advance=1)

    # Limpa o console e exibe feedback
    limpar_console()
    console.print(f"[bold cyan]Arquivo processado: {nome_arquivo}[/bold cyan]")
    console.print(f"[bold cyan]Linhas duplicadas detectadas: {len(pares)}[/bold cyan]")


def extrair_dados_concorrente(pasta, padrao, nome_arquivo_saida, tipo_extracao):
    """
    Extrai dados de arquivos em uma pasta de forma concorrente.
    """
    arquivos_txt = [f for f in os.listdir(pasta) if f.endswith('.txt')]
    total_arquivos = len(arquivos_txt)

    if total_arquivos == 0:
        console.print("[bold red]Nenhum arquivo .txt encontrado na pasta selecionada.[/bold red]")
        return

    nome_arquivo_saida = gerar_nome_arquivo(nome_arquivo_saida)

    # Mensagem inicial sobre a extração
    console.print(f"[bold green]Iniciando extração de {tipo_extracao}...[/bold green]")
    time.sleep(2)  # Exibe a mensagem por 2 segundos

    console.print("[bold yellow]Os dados estão sendo processados...[/bold yellow]")
    console.print("[bold green]A extração foi iniciada![/bold green]")
    time.sleep(2)

    while True:
        try:
            max_threads = int(console.input("[bold cyan]Digite o número de threads (1-7): [/bold cyan]"))
            if 1 <= max_threads <= 7:
                break
            else:
                console.print("[red]Por favor, insira um número entre 1 e 7.[/red]")
        except ValueError:
            console.print("[red]Entrada inválida. Por favor, insira um número válido.[/red]")

    # Criar a tabela de progresso e gerenciar com Live
    with Live(console=console, refresh_per_second=10) as live:
        progress = Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeRemainingColumn(),
            console=console,
        )
        progresso_total = progress.add_task("[green]Processando arquivos...", total=total_arquivos)

        # Adicionando barra de progresso total
        progresso_linhas = progress.add_task("[blue]Progresso total de linhas...", total=0)

        def generate_progress_table():
            """
            Gera uma tabela para exibir o progresso da extração de dados.
            """
            table = Table(title=f"Progresso da Extração de {tipo_extracao}", show_lines=True)
            table.add_column("Tipo de Dado", style="cyan", justify="center")
            table.add_column("Total Encontrados", style="magenta", justify="center")
            table.add_column("Linhas Duplicadas", style="yellow", justify="center")
            table.add_column("Linhas Finais", style="green", justify="center")
            table.add_row(
                tipo_extracao,
                str(total_pares_encontrados[tipo_extracao]),
                str(total_pares_encontrados.get("duplicados", 0)),
                str(total_pares_encontrados.get("finais", 0)),
            )
            return table

        lock = threading.Lock()
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = [
                executor.submit(
                    processar_arquivo, pasta, arquivo, padrao, nome_arquivo_saida, progresso_total, lock, progress, tipo_extracao
                )
                for arquivo in arquivos_txt
            ]

            while not all(f.done() for f in futures):
                # Atualiza a tabela e barra de progresso total
                live.update(generate_progress_table())
                progress.update(progresso_linhas, total=sum(total_pares_encontrados.values()))
                time.sleep(0.5)

        # Atualiza a tabela no final
        live.update(generate_progress_table())

    console.print(f"[bold green]Extração concluída! Total de {tipo_extracao}(s) encontrados: {total_pares_encontrados[tipo_extracao]}[/bold green]")
    time.sleep(5)


def selecionar_pasta():
    """Abre uma janela do Explorer para o usuário selecionar uma pasta."""
    limpar_console()
    console.rule("[bold blue]Seleção de Diretório")
    console.print("[bold cyan]Por favor, selecione o diretório que contém os arquivos .txt para extração.[/bold cyan]")
    time.sleep(2)

    root = tk.Tk()
    root.attributes("-topmost", True)
    root.withdraw()
    pasta_selecionada = filedialog.askdirectory(title="Selecione a pasta com arquivos .txt")
    root.quit()
    root.destroy()
    return pasta_selecionada


def tela_inicial():
    """Exibe a tela inicial com opções para iniciar ou encerrar o programa."""
    limpar_console()
    console.rule("[bold blue]Tela Inicial")
    console.print("[bold green]Desenvolvedor: OTO Dev[/bold green]")
    console.print("[bold green]Projeto: NEXUS DEV CENTER[/bold green]")
    console.print("[1] [green]Iniciar Programa[/green]")
    console.print("[2] [red]Encerrar Programa[/red]")
    while True:
        opcao = console.input("[bold cyan]Escolha uma opção (1 ou 2): [/bold cyan]")
        if opcao == '1':
            return True
        elif opcao == '2':
            console.print("[bold red]Encerrando o programa...[/bold red]")
            exit()
        else:
            console.print("[bold red]Opção inválida. Tente novamente.[/bold red]")


def menu():
    """Exibe o menu principal."""
    limpar_console()
    console.rule("[bold blue]Menu Principal")
    console.print("[1] [green]Extrair CPF:Senha[/green]")
    console.print("[2] [green]Extrair CNPJ:Senha[/green]")
    console.print("[3] [green]Extrair Email:Senha[/green]")
    console.print("[4] [yellow]Créditos[/yellow]")
    console.print("[5] [red]Sair[/red]")
    return console.input("[bold cyan]Escolha uma opção (1, 2, 3, 4 ou 5): [/bold cyan]")


def menu_creditos():
    """Exibe os créditos do desenvolvedor."""
    limpar_console()
    console.rule("[bold blue]Créditos[/bold blue]")
    console.print("[bold green]Desenvolvedor: OTO Dev[/bold green]")
    console.print("[bold green]Projeto: NEXUS DEV CENTER[/bold green]")
    console.print("[bold yellow]Descrição: Ferramenta para extração de dados sensíveis de arquivos .txt.[/bold yellow]")
    console.print("[bold cyan]Agradecimentos especiais a todos que contribuíram para este projeto![/bold cyan]")
    console.print("\n[bold cyan]Pressione qualquer tecla para voltar ao menu principal...[/bold cyan]")
    console.input()


def extrair_cpf_senha(pasta):
    """Extrai CPF e senhas de arquivos .txt em uma pasta."""
    padrao_cpf_senha = re.compile(r'(\d{3}\.\d{3}\.\d{3}-\d{2}|\d{11}):(.+?)(?::|$)')
    nome_arquivo_saida = 'cpf_senhas_extraidas.txt'
    extrair_dados_concorrente(pasta, padrao_cpf_senha, nome_arquivo_saida, "CPF")


def extrair_cnpj_senha(pasta):
    """Extrai CNPJ e senhas de arquivos .txt em uma pasta."""
    padrao_cnpj_senha = re.compile(r'(\d{14}):(.+?)(?::|$)')
    nome_arquivo_saida = 'cnpj_senhas_extraidas.txt'
    extrair_dados_concorrente(pasta, padrao_cnpj_senha, nome_arquivo_saida, "CNPJ")


def extrair_email_senha(pasta):
    """Extrai email e senhas de arquivos .txt em uma pasta."""
    padrao_email_senha = re.compile(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}):(.+?)(?::|$)')
    nome_arquivo_saida = 'emails_senhas_extraidas.txt'
    extrair_dados_concorrente(pasta, padrao_email_senha, nome_arquivo_saida, "Email")


if __name__ == "__main__":
    if tela_inicial():
        while True:
            opcao = menu()
            if opcao == '1':
                pasta = selecionar_pasta()
                if pasta:
                    extrair_cpf_senha(pasta)
            elif opcao == '2':
                pasta = selecionar_pasta()
                if pasta:
                    extrair_cnpj_senha(pasta)
            elif opcao == '3':
                pasta = selecionar_pasta()
                if pasta:
                    extrair_email_senha(pasta)
            elif opcao == '4':
                menu_creditos()
            elif opcao == '5':
                console.print("[bold red]Encerrando o programa...[/bold red]")
                break
            else:
                console.print("[bold red]Opção inválida. Tente novamente.[/bold red]")
