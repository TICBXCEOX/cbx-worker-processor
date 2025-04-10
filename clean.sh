#!/bin/bash

echo "INFORMAÇÕES ILUSTRATIVAS"
echo "------------------------"
echo "INFO: Para limpeza geral (containers parados, imagens não usadas, redes e volumes não usados): docker system prune -a --volumes "
# docker system prune -a --volumes
echo "INFO: Para corrigir dns docker: echo "nameserver 8.8.8.8" | sudo tee /etc/resolv.conf > /dev/null"
# echo "nameserver 8.8.8.8" | sudo tee /etc/resolv.conf > /dev/null
echo "INFO: Para proteger o arquivo contra sobrescrita: sudo chattr +i /etc/resolv.conf"
# sudo chattr +i /etc/resolv.conf
echo "INFO: Isso impede que o WSL atualize o arquivo, então você pode reverter com: sudo chattr -i /etc/resolv.conf"
echo "INFO: Para verificar o IP do WSL (postgre instalado no windows): ip route | grep default	"
echo "INFO: Descobrir o IP do Ubuntu para passar no container (postgre instalado no ubuntu): ip addr show eth0 | grep inet"

echo "-------------------------------------"
echo "EXECUTANDO A LIMPEZA: docker_clean.sh"
echo "-------------------------------------"

echo "🧼 Limpando todos os containers..."
docker rm -f $(docker ps -aq)

echo "🧼 Limpando todas as imagens..."
docker rmi -f $(docker images -aq)

echo "✅ Limpeza concluída!"

# echo "-------------------------------------"
# echo "EXECUTANDO docker-compose"
# echo "-------------------------------------"
# sh build.sh

echo "-------------------------------------"
echo "FINALIZADO! 👍"
echo "-------------------------------------"