import subprocess
import re

def get_scontrol(cmd):
    return subprocess.check_output(cmd, shell=True, universal_newlines=True)

def parse_partitions(raw):
    partitions = {}
    all_accounts = set()
    for part in re.split(r"PartitionName=", raw)[1:]:
        lines = part.splitlines()
        pname = lines[0].split()[0]
        allow_accounts = re.search(r"AllowAccounts=([^\s]+)", part)
        qos = re.search(r"AllowQos=([^\s]+)", part)
        nodes = re.search(r"Nodes=([^\s]+)", part)
        tres = re.search(r"TRES=([^\n]+)", part)
        accounts_list = allow_accounts.group(1).split(",") if allow_accounts else []
        for a in accounts_list:
            all_accounts.add(a)
        partitions[pname] = {
            "accounts": accounts_list,
            "qos": qos.group(1) if qos else "",
            "nodes": nodes.group(1).split(",") if nodes else [],
            "tres": tres.group(1) if tres else ""
        }
    return partitions, sorted(list(all_accounts))

def parse_nodes(raw):
    nodes = {}
    for block in re.split(r"NodeName=", raw)[1:]:
        lines = block.splitlines()
        nname = lines[0].split()[0]
        cpu_tot = re.search(r"CPUTot=(\d+)", block)
        cpu_alloc = re.search(r"CPUAlloc=(\d+)", block)
        gres_match = re.search(r"Gres=([^\n]+)", block)
        part_match = re.search(r"Partitions=([^\s]+)", block)
        alloc_tres = re.search(r"AllocTRES=([^\n]+)", block)
        nodes[nname] = {
            "cpus_total": int(cpu_tot.group(1)) if cpu_tot else 0,
            "cpus_alloc": int(cpu_alloc.group(1)) if cpu_alloc else 0,
            "gres": gres_match.group(1).strip() if gres_match else "",
            "partitions": part_match.group(1).split(",") if part_match else [],
            "alloc_tres": alloc_tres.group(1) if alloc_tres else "",
        }
    return nodes

def prompt_user(all_groups):
    print("Available user accounts on this HPC:", ", ".join(all_groups))
    groups = input("Enter your account(s) (comma-separated, e.g. mdbf,users): ").strip()
    group_list = [g.strip() for g in groups.split(",") if g.strip()]
    cpu = int(input("CPUs needed: "))
    gpu = int(input("GPUs needed (enter 0 if not necessarily needed): "))
    if gpu > 0:
        model = input("GPU model (leave empty if any): ").strip()
    else:
        model = ""
    return group_list, cpu, gpu, model


def get_free_gpus(gres_line, alloc_tres_line, requested_model):
    total = {}
    alloc = {}

    # Parse total GPUs from Gres line
    for model, n in re.findall(r"gpu:([\w_]+):(\d+)", gres_line):
        total[model] = int(n)
    # Parse allocated GPUs from AllocTRES line
    if alloc_tres_line:
        for model, n in re.findall(r"gpu:([\w_]+)=(\d+)", alloc_tres_line):
            alloc[model] = int(n)
    # Compute free for requested model
    free = 0
    model_used = ""
    if requested_model:
        for model in total:
            if requested_model.lower() in model.lower():
                free = total[model] - alloc.get(model, 0)
                model_used = model
                break
    else:  # Any model is acceptable
        for model in total:
            f = total[model] - alloc.get(model, 0)
            if f > 0:
                free = f
                model_used = model
                break
    return free, model_used

def filter_nodes(nodes, partitions, group_list, cpu_needed, gpu_needed, gpu_model):
    results = []
    for nodename, ndata in nodes.items():
        available_cpus = ndata["cpus_total"] - ndata["cpus_alloc"]
        if available_cpus < cpu_needed:
            continue
        for part in ndata["partitions"]:
            if part not in partitions: continue
            p = partitions[part]
            # Accept any match in user groups
            if not (set(group_list) & set(p["accounts"]) or "ALL" in p["accounts"]):
                continue
            free_gpu, found_model = get_free_gpus(ndata["gres"], ndata["alloc_tres"], gpu_model)
            if gpu_needed > 0 and free_gpu < gpu_needed:
                continue
            results.append({
                "Node": nodename,
                "Partition": part,
                "QoS": p["qos"],
                "FreeCPUs": available_cpus,
                "GPU#": free_gpu,
                "GPU Model": found_model if free_gpu > 0 else "",
            })
    return results

def print_table(rows):
    if not rows:
        print("No matching nodes found.")
        return
    headers = ["Node", "Partition", "QoS", "FreeCPUs", "GPU#", "GPU Model"]
    colwidths = [max(len(str(row[h])) for row in rows+[dict(zip(headers,headers))]) for h in headers]
    fmt = " | ".join(f"{{:{w}}}" for w in colwidths)
    print(fmt.format(*headers))
    print("-+-".join('-'*w for w in colwidths))
    for row in rows:
        print(fmt.format(*[row[h] for h in headers]))

def main():
    partitions_raw = get_scontrol("scontrol show partition")
    nodes_raw = get_scontrol("scontrol show node")

    partitions, all_groups = parse_partitions(partitions_raw)
    nodes = parse_nodes(nodes_raw)
    group_list, cpu, gpu, model = prompt_user(all_groups)
    results = filter_nodes(nodes, partitions, group_list, cpu, gpu, model)
    print_table(results)

if __name__ == "__main__":
    main()
