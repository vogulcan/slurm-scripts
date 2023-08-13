import subprocess
import random

COLORS = [
    #console color
'\033[0m', # reset
'\033[31m', # red
'\033[32m', # green
'\033[33m', # orange
'\033[34m', # blue
'\033[35m', # purple
'\033[36m', # cyan
'\033[37m', # gray
'\033[1;30m', # dark gray
'\033[1;31m', # light red
'\033[1;32m', # light green
'\033[1;33m', # yellow
'\033[1;34m', # light blue
'\033[1;35m', # light purple
'\033[1;36m', # light cyan
]

def parse_def_mem():
    config_out = subprocess.check_output(['scontrol', 'show', 'config']).decode('utf-8')
    for line in config_out.split('\n'):
        if 'DefMemPerCPU' in line:
            return int(line.split('=')[1])

def parse_partitions(partitions_output):
    partitions = partitions_output.strip().split('\n\n')
    partition_info = {}

    for partition in partitions:
        lines = partition.split('\n')
        partition_name = lines[0].split('=')[1]
        if partition_name == 'debug':
            continue
        partition_allow = lines[1].split()
        partition_user = partition_allow[1].split('=')[1].split(',')
        partition_user = [user for user in partition_user if user != 'admin']

        partition_qos = partition_allow[2].split('=')[1].split(',')
        partition_qos = [qos for qos in partition_qos if qos != 'admin']
        partition_nodes = lines[5].split('=')[1]
        partition_nodes_raw = partition_nodes.replace('[', '').replace(']', '').replace('cn', '').split(',')
        partition_nodes = []
        for node in partition_nodes_raw:
            if node.isdigit():
                curr_node = int(node)
                if curr_node < 10:
                    partition_nodes.append(f'cn0{curr_node}')
                else:
                    partition_nodes.append(f'cn{curr_node}')
            elif '-' in node:
                start, end = node.split('-')
                if int(end) < 10: 
                    partition_nodes.extend([f'cn0{i}' for i in range(int(start), int(end)+1)])
                else:
                    for i in range(int(start), int(end)+1):
                        if i < 10:
                            partition_nodes.append(f'cn0{i}')
                        else:
                            partition_nodes.append(f'cn{i}')
            else:
                partition_nodes.append(node)
        partition_info[partition_name] = {'qos': partition_qos, 'user': partition_user, 'nodes': partition_nodes}
    return partition_info

def filter_linker(linker, node_name):
    p_dict = {}
    for k, v in linker.items():
        if node_name in v['nodes']:
            p_dict[k] = linker[k]
    return p_dict

def print_table(table, current_colors):
    longest_cols = [
        (max([len(str(row[i])) for row in table]) + 3)
        for i in range(len(table[0]))
    ]
    row_format = "".join(["{:>" + str(longest_col) + "}" for longest_col in longest_cols])
    for row, c in zip(table, current_colors):
        print(c+row_format.format(*row))

def parse_nodes(nodes_output, linker, restrictions):
    all_node_info = nodes_output.strip().split('\n\n')
    table = [['Node', 'Partition', 'Account', 'QoS', '#CPU Cores Avail.', '#GPU Avail.', 'GPU Name', 'Memory Avail.(GB)']]
    colors_for_table = [COLORS[0]]
    for node in all_node_info:
        
        lines = node.split('\n')
        node_name = lines[0].split('=')[1].split()[0]
        
        cpus_alloc = int(lines[1].split()[0].split('=')[1])
        cpus_all = int(lines[1].split()[1].split('=')[1])
        cpus_avail = cpus_all - cpus_alloc
        cores_avail = cpus_avail // 2

        current_mem_avail = int(lines[7].split()[2].split('=')[1]) // 1024

        total_gpus = 0
        gpu_name = 'none'

        if 'gpu' in lines[-6]:
            total_gpus = int(lines[-6].split(',')[-1].split('=')[1])
            gpu_name = lines[-6].split(',')[-1].split(':')[1].split('=')[0]

        if 'gpu' in lines[-5]:
            total_gpus -= int(lines[-5].split(',')[-1].split('=')[1])
        
        check_cpu = restrictions['required_cpus'] <= cores_avail
        check_gpu = restrictions['required_gpus'] <= total_gpus
        
        color_for_this_node = random.choice(COLORS[1:])
        if check_cpu and check_gpu:
            n_part = 0
            for partition, data in filter_linker(linker, node_name).items():
                account_available = [user for user in restrictions['user_account'] if user in data['user']]
                if len(account_available) > 0:
                    n_part += 1
                    table.append([node_name, partition, ",".join(account_available), ",".join(data['qos']), cores_avail, total_gpus, gpu_name, current_mem_avail])
    
            colors_for_table.extend([color_for_this_node] * n_part)
    if len(table) > 1:
        print(f'\nReminder: Default Memory per CPU core within the SLURM config is {parse_def_mem()} MB [DefMemPerCPU].\n')
        print_table(table, colors_for_table)
    else:
        print("No nodes available. Please try again with different restrictions.")

def main():
    restrictions = {"user_account" : [acc for acc in input("Enter your user account(s) \nSeparate with commas (,) if multiple or 'all' for all accounts:\t").split(',')],
    "required_cpus" : int(input("Enter the number of CPU cores (0 for no filter):\t")),
    "required_gpus" : int(input("Enter the number of GPUs (0 for no filter):\t")),
    }

    print("\nYou have entered the following: ")
    for entered_key, val in zip(['Accounts', "#CPU core", "#GPU" ], restrictions.values()):
        print(f"\t{entered_key}: {' '.join(val) if isinstance(val, list) else val}")

    partitions_output = subprocess.check_output(['scontrol', 'show', 'partition']).decode('utf-8')
    nodes_output = subprocess.check_output(['scontrol', 'show', 'node']).decode('utf-8')
    partition_data = parse_partitions(partitions_output)

    all_possible_accounts = set([user for _, data in partition_data.items() for user in data['user']])
    if restrictions['user_account'] == ['all']:
        restrictions['user_account'] = list(all_possible_accounts)
    account_check = len(set(restrictions['user_account']) & all_possible_accounts)
    account_check_list = [1 if user in all_possible_accounts else 0 for user in restrictions['user_account']]
    if account_check:
        not_valid_idx_list = [i for i, e in enumerate(account_check_list) if e == 0]
        if len(not_valid_idx_list) > 0:
            print(f"Account(s) {', '.join([restrictions['user_account'][i] for i in not_valid_idx_list])} is/are not present in this HPC. Please check if you typed correctly.\nPossible Accounts\n {' '.join(all_possible_accounts)}", COLORS[0])
        else:
            parse_nodes(nodes_output, partition_data, restrictions=restrictions)
    else:
        print(f"None of your account(s) are allowed to use any of the partitions. Please contact the HPC admin, or check if you typed correctly.\nPossible Accounts\n {' '.join(all_possible_accounts)}", COLORS[0])
    print("\n")

if __name__ == "__main__":
    main()