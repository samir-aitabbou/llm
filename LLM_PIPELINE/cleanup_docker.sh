#!/bin/bash

# Ask for confirmation
read -p "Are you sure you want to completely clean up Docker? This action cannot be undone. (y/n) " -n 1 -r
echo    # move to a new line

if [[ $REPLY =~ ^[Yy]$ ]]
then
    # Stop all containers
    echo "Stopping all containers..."
    docker stop $(docker ps -a -q)

    # Remove all containers
    echo "Removing all containers..."
    docker rm $(docker ps -a -q)

    # Remove all docker networks
    echo "Removing all networks..."
    docker network prune -f

    # Remove all docker images
    echo "Removing all images..."
    docker rmi $(docker images -q) -f

    # Clean up dangling volumes if you want to remove volumes as well
    echo "Removing all unused volumes..."
    docker volume prune -f

    # Clean up any leftover resources
    echo "Final cleanup..."
    docker system prune -f

    echo "Docker cleanup complete."
else
    echo "Cleanup canceled by user."
fi

