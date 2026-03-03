import json

# We first need to run: dragon-network-config --wlm slurm --output-to-json
# this script reads from the slurm.json the ip addresses of the allocation 
# and prints a list that can be then used from the manual_launch.sh to set up a ray cluster

def get_ip_addresses(json_data):
    ip_addresses = []

    for key, value in json_data.items():
        ip_addrs = value["ip_addrs"]
        for ip_addr in ip_addrs:
            ip = ip_addr.split(':')[0]
            ip_addresses.append(ip)

    return ip_addresses

def main():
    with open('slurm.json', 'r') as json_file:
        json_data = json.load(json_file)

    ip_addresses = get_ip_addresses(json_data)
    print(' '.join(ip_addresses))

if __name__ == "__main__":
    main()

