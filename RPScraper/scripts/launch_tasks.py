#!/usr/bin/env python3

import boto3
import datetime as dt
import argparse
from typing import List, Dict, Any
import json
from pathlib import Path

def parse_date(date_str: str) -> dt.date:
    """Parse date string in YYYY-MM-DD format"""
    return dt.datetime.strptime(date_str, '%Y-%m-%d').date()

def generate_date_ranges(start_date: dt.date, end_date: dt.date) -> List[Dict[str, str]]:
    """Generate date ranges split by year"""
    ranges = []
    current_date = start_date
    
    while current_date <= end_date:
        year_end = dt.date(current_date.year, 12, 31)
        range_end = min(year_end, end_date)
        
        ranges.append({
            'start_date': current_date.strftime('%Y-%m-%d'),
            'end_date': range_end.strftime('%Y-%m-%d')
        })
        
        if range_end == end_date:
            break
            
        current_date = range_end + dt.timedelta(days=1)
    
    return ranges

def get_default_vpc_resources(region: str) -> Dict[str, str]:
    """Get the default VPC, subnet, and security group"""
    session = boto3.Session(profile_name='personal', region_name=region)
    ec2 = session.client('ec2')
    
    # Get default VPC
    vpcs = ec2.describe_vpcs(Filters=[{'Name': 'isDefault', 'Values': ['true']}])
    if not vpcs['Vpcs']:
        raise Exception("No default VPC found")
    vpc_id = vpcs['Vpcs'][0]['VpcId']
    
    # Get first subnet in default VPC
    subnets = ec2.describe_subnets(Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}])
    if not subnets['Subnets']:
        raise Exception("No subnets found in default VPC")
    subnet_id = subnets['Subnets'][0]['SubnetId']
    
    # Get default security group
    sgs = ec2.describe_security_groups(
        Filters=[
            {'Name': 'vpc-id', 'Values': [vpc_id]},
            {'Name': 'group-name', 'Values': ['default']}
        ]
    )
    if not sgs['SecurityGroups']:
        raise Exception("No default security group found")
    sg_id = sgs['SecurityGroups'][0]['GroupId']
    
    return {
        'vpc_id': vpc_id,
        'subnet_id': subnet_id,
        'security_group_id': sg_id
    }

def launch_fargate_task(ecs_client: Any, cluster: str, task_definition: str, 
                       subnet: str, security_group: str, 
                       start_date: str, end_date: str, country: str) -> None:
    """Launch a Fargate task with the specified parameters"""
    
    network_config = {
        'awsvpcConfiguration': {
            'subnets': [subnet],
            'securityGroups': [security_group],
            'assignPublicIp': 'ENABLED'
        }
    }
    
    container_overrides = {
        'containerOverrides': [{
            'name': 'rpscrape-regenerate-data',
            'environment': [
                {'name': 'START_DATE', 'value': start_date},
                {'name': 'END_DATE', 'value': end_date},
                {'name': 'COUNTRIES', 'value': country},
                {'name': 'FORCE', 'value': 'true'}
            ]
        }]
    }
    
    try:
        response = ecs_client.run_task(
            cluster=cluster,
            taskDefinition=task_definition,
            networkConfiguration=network_config,
            overrides=container_overrides,
            launchType='FARGATE'
        )
        print(f"Launched task for {country} from {start_date} to {end_date}")
        print(f"Task ARN: {response['tasks'][0]['taskArn']}")
    except Exception as e:
        print(f"Error launching task for {country} {start_date}-{end_date}: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='Launch Fargate tasks for data scraping')
    parser.add_argument('--cluster', required=True, help='ECS cluster name')
    parser.add_argument('--task-definition', default='rpscrape-regenerate-data', 
                       help='Task definition name')
    parser.add_argument('--region', default='eu-west-1', help='AWS region')
    
    args = parser.parse_args()
    
    # Initialize AWS session with personal profile
    session = boto3.Session(profile_name='personal', region_name=args.region)
    ecs_client = session.client('ecs')
    
    # Get default VPC resources
    try:
        vpc_resources = get_default_vpc_resources(args.region)
        print(f"\nUsing default VPC resources:")
        print(f"VPC ID: {vpc_resources['vpc_id']}")
        print(f"Subnet ID: {vpc_resources['subnet_id']}")
        print(f"Security Group ID: {vpc_resources['security_group_id']}\n")
    except Exception as e:
        print(f"Error getting default VPC resources: {str(e)}")
        return
    
    # Set fixed parameters
    start_date = parse_date('2008-05-28')
    end_date = dt.date.today()
    countries = ['gb', 'ire']
    
    print(f"Generating tasks from {start_date} to {end_date}")
    print(f"Countries: {', '.join(countries)}")
    
    # Generate date ranges
    date_ranges = generate_date_ranges(start_date, end_date)
    total_tasks = len(date_ranges) * len(countries)
    
    print(f"\nTotal number of tasks to launch: {total_tasks}")
    print(f"Date ranges: {len(date_ranges)}")
    print("Preview of tasks:")
    for country in countries:
        print(f"\n{country}:")
        for date_range in date_ranges[:2]:  # Show first 2 ranges as preview
            print(f"  {date_range['start_date']} to {date_range['end_date']}")
        if len(date_ranges) > 2:
            print(f"  ... and {len(date_ranges)-2} more ranges")
    
    print("\nLaunching tasks...")
    tasks_launched = 0
    
    # Launch tasks for each country and date range
    for country in countries:
        for date_range in date_ranges:
            launch_fargate_task(
                ecs_client=ecs_client,
                cluster=args.cluster,
                task_definition=args.task_definition,
                subnet=vpc_resources['subnet_id'],
                security_group=vpc_resources['security_group_id'],
                start_date=date_range['start_date'],
                end_date=date_range['end_date'],
                country=country
            )
            tasks_launched += 1
            print(f"Progress: {tasks_launched}/{total_tasks} tasks launched")

if __name__ == '__main__':
    main()
