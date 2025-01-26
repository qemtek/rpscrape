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
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--countries', required=True, help='Comma-separated list of countries')
    parser.add_argument('--cluster', required=True, help='ECS cluster name')
    parser.add_argument('--subnet', required=True, help='Subnet ID')
    parser.add_argument('--security-group', required=True, help='Security group ID')
    parser.add_argument('--task-definition', default='rpscrape-regenerate-data', 
                       help='Task definition name')
    parser.add_argument('--region', default='eu-west-1', help='AWS region')
    
    args = parser.parse_args()
    
    # Initialize AWS client
    ecs_client = boto3.client('ecs', region_name=args.region)
    
    # Parse dates
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    
    # Get list of countries
    countries = [c.strip() for c in args.countries.split(',')]
    
    # Generate date ranges
    date_ranges = generate_date_ranges(start_date, end_date)
    
    # Launch tasks for each country and date range
    for country in countries:
        for date_range in date_ranges:
            launch_fargate_task(
                ecs_client=ecs_client,
                cluster=args.cluster,
                task_definition=args.task_definition,
                subnet=args.subnet,
                security_group=args.security_group,
                start_date=date_range['start_date'],
                end_date=date_range['end_date'],
                country=country
            )

if __name__ == '__main__':
    main()
