# 🏦 Real-Time Banking Data Processing Platform

> Pipeline de traitement des données bancaires en temps réel — Kubernetes · Kafka · Flink · MinIO · Terraform

---

## 📋 Table des matières

- [Vue d'ensemble](#-vue-densemble)
- [Architecture](#-architecture)
- [Stack technique](#-stack-technique)
- [Structure du projet](#-structure-du-projet)
- [Infrastructure](#-infrastructure)
- [Déploiement](#-déploiement)
- [Sécurité](#-sécurité)
- [Statut des tâches](#-statut-des-tâches)
- [Validation](#-validation)

---

## 🎯 Vue d'ensemble

Ce projet implémente un pipeline de traitement des données bancaires en temps réel en s'appuyant sur une infrastructure Kubernetes locale (Minikube), provisionnée avec Terraform. L'architecture suit une approche **event-driven** avec ingestion via **Apache Kafka**, traitement via **Apache Flink**, et stockage objet via **MinIO**.

### Objectifs

- Ingérer des flux de transactions bancaires en temps réel
- Détecter des anomalies et patterns frauduleux à la volée
- Stocker les résultats enrichis dans un data lake MinIO
- Offrir une infrastructure reproductible, sécurisée et versionnée

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Minikube Cluster                         │
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐  │
│  │  Namespace   │    │  Namespace   │    │   Namespace      │  │
│  │   kafka      │    │    flink     │    │     minio        │  │
│  │              │    │              │    │                  │  │
│  │  ┌────────┐  │    │  ┌────────┐  │    │  ┌────────────┐  │  │
│  │  │ Broker │  │───▶│  │  Job   │  │───▶│  │  Object    │  │  │
│  │  │ Kafka  │  │    │  │Manager │  │    │  │  Storage   │  │  │
│  │  └────────┘  │    │  └────────┘  │    │  └────────────┘  │  │
│  │  ┌────────┐  │    │  ┌────────┐  │    │                  │  │
│  │  │Zookeep.│  │    │  │  Task  │  │    │                  │  │
│  │  └────────┘  │    │  │Manager │  │    │                  │  │
│  └──────────────┘    │  └────────┘  │    └──────────────────┘  │
│                      └──────────────┘                           │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Network Policies + RBAC                    │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

**Flux de données :**

```
Transactions bancaires ──▶ Kafka Topics ──▶ Flink Jobs ──▶ MinIO Buckets
     (producers)              (streaming)      (processing)    (storage)
```

---

## 🛠️ Stack technique

| Composant | Technologie | Version | Rôle |
|-----------|-------------|---------|------|
| Orchestration | Kubernetes (Minikube) | v1.28+ | Cluster local |
| Infrastructure as Code | Terraform | v1.6+ | Provisionnement |
| Package Manager K8s | Helm | v3.12+ | Déploiement des charts |
| Message Broker | Apache Kafka | v3.5+ | Ingestion temps réel |
| Stream Processing | Apache Flink | v1.18+ | Traitement des flux |
| Object Storage | MinIO | RELEASE.2024+ | Data lake |
| CLI | kubectl | v1.28+ | Gestion du cluster |
| Containerisation | Docker | v24+ | Runtime des conteneurs |
| Virtualisation | WSL2 | — | Environnement Linux |

---

## 📁 Structure du projet

```
banking-realtime-platform/
│
├── infrastructure/
│   ├── terraform/
│   │   ├── main.tf              # Provider et backend Terraform
│   │   ├── namespaces.tf        # Namespaces Kubernetes
│   │   ├── kafka.tf             # Déploiement Kafka + Zookeeper
│   │   ├── minio.tf             # Déploiement MinIO
│   │   ├── flink.tf             # Déploiement Flink (JobManager + TaskManager)
│   │   ├── network_policies.tf  # Politiques réseau inter-namespaces
│   │   ├── rbac.tf              # Rôles et permissions Flink
│   │   └── variables.tf         # Variables Terraform
│   │
│   └── helm/
│       └── values/              # Override values des charts Helm
│
├── jobs/
│   └── flink/                   # Jobs Flink (à venir)
│
├── docs/
│   └── architecture.md          # Documentation architecture détaillée
│
└── README.md
```

---

## ⚙️ Infrastructure

### Namespaces Kubernetes

| Namespace | Description |
|-----------|-------------|
| `kafka` | Broker Kafka et Zookeeper |
| `flink` | JobManager et TaskManagers Flink |
| `minio` | Stockage objet MinIO |

### Ressources Minikube

```bash
minikube start --cpus=2 --memory=3072 --driver=docker
```

| Ressource | Valeur |
|-----------|--------|
| CPUs | 2 vCPUs |
| Mémoire | 3072 MB |
| Driver | Docker |

---

## 🚀 Déploiement

### Prérequis

```bash
# Vérifier les outils installés
wsl --version
docker --version
minikube version
kubectl version --client
helm version
terraform version
```

### 1. Démarrer le cluster

```bash
minikube start --cpus=2 --memory=3072 --driver=docker
```

### 2. Initialiser Terraform

```bash
cd infrastructure/terraform
terraform init
```

### 3. Planifier et appliquer

```bash
# Visualiser les changements
terraform plan

# Déployer l'infrastructure complète
terraform apply -auto-approve
```

### 4. Vérifier les pods

```bash
# Vérifier tous les namespaces
kubectl get pods -A

# Vérifier Kafka
kubectl get pods -n kafka

# Vérifier Flink
kubectl get pods -n flink

# Vérifier MinIO
kubectl get pods -n minio
```

### Alias kubectl recommandés

```bash
alias k='kubectl'
alias kgp='kubectl get pods'
alias kga='kubectl get pods -A'
alias kns='kubectl config set-context --current --namespace'
```

---

## 🔒 Sécurité

### Network Policies

Les politiques réseau restreignent la communication inter-namespaces selon le principe du moindre privilège :

- **Kafka** : accessible uniquement depuis le namespace `flink`
- **MinIO** : accessible depuis `flink` uniquement pour l'écriture des résultats
- **Flink** : communication interne JobManager ↔ TaskManager autorisée

### RBAC

Un `ServiceAccount` dédié est créé pour Flink avec les permissions minimales nécessaires :

```yaml
# Rôles accordés au ServiceAccount Flink
- get, list, watch : pods, services, configmaps
- create, delete   : pods (pour la gestion des TaskManagers)
```

---

## 📊 Statut des tâches

| Phase | Tâche | Statut |
|-------|-------|--------|
| Infrastructure | Installer WSL2 + outils (Docker, Minikube, Helm, Terraform, kubectl) | ✅ Terminé |
| Infrastructure | Configurer Minikube | ✅ Terminé |
| Infrastructure | Fichiers Terraform (main.tf, namespaces.tf, kafka.tf, minio.tf, flink.tf) | ✅ Terminé |
| Infrastructure | `terraform apply` — Namespaces + Kafka + MinIO + Flink déployés | ✅ Terminé |
| Infrastructure | Network Policies + RBAC | 🔄 En cours |
| Infrastructure | Push GitHub | 🔄 En cours |
| Infrastructure | Test & Validation | ⏳ En attente |
| Documentation | Doc du repo git | 🔄 En cours |

---

## ✅ Validation

### Checklist de validation infrastructure

```bash
# 1. Namespaces créés
kubectl get namespaces | grep -E "kafka|flink|minio"

# 2. Pods en état Running
kubectl get pods -n kafka
kubectl get pods -n flink
kubectl get pods -n minio

# 3. Services exposés
kubectl get svc -A

# 4. Test connectivité Kafka → Flink
kubectl exec -n flink <flink-pod> -- nc -zv kafka.kafka.svc.cluster.local 9092

# 5. Test connectivité Flink → MinIO
kubectl exec -n flink <flink-pod> -- nc -zv minio.minio.svc.cluster.local 9000
```

### Résultats attendus

- ✅ 3 namespaces actifs : `kafka`, `flink`, `minio`
- ✅ Tous les pods en état `Running`
- ✅ Kafka broker joignable depuis Flink
- ✅ MinIO accessible depuis Flink
- ✅ Network Policies appliquées
- ✅ RBAC configuré pour le ServiceAccount Flink

---

## 👥 Équipe

| Rôle | Responsabilité |
|------|---------------|
| Infrastructure | Provisionnement Terraform, configuration Kubernetes |
| Validation | Haitam — Tests et validation de l'infrastructure déployée |

---

*Projet de traitement des données bancaires en temps réel — Infrastructure v1.0*
