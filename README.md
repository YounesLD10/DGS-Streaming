# DGS-Streaming — PoC Traitement Temps Réel des Paiements

> **Proof of Concept** — Pipeline de traitement en temps réel des transactions de paiement,
> déployé sur Minikube pour HPS Group (Casablanca).

---

## Table des matières

1. [Vue d'ensemble](#1-vue-densemble)
2. [Architecture](#2-architecture)
3. [Prérequis](#3-prérequis)
4. [Structure du projet](#4-structure-du-projet)
5. [Composants déployés](#5-composants-déployés)
6. [Déploiement](#6-déploiement)
7. [Vérification de l'état du cluster](#7-vérification-de-létat-du-cluster)
8. [Exécution du producteur](#8-exécution-du-producteur)
9. [Sécurité réseau et RBAC](#9-sécurité-réseau-et-rbac)
10. [Dépannage — Problèmes rencontrés](#10-dépannage--problèmes-rencontrés)
11. [Suppression de l'infrastructure](#11-suppression-de-linfrastructure)

---

## 1. Vue d'ensemble

Ce PoC démontre un pipeline de streaming temps réel pour le traitement des transactions de paiement bancaire. Il simule un flux de données de type **Change Data Capture (CDC)** à partir d'un fichier CSV Kaggle (jeu de données de transactions de cartes de crédit), chiffre les données avec **Fernet (AES-128-CBC + HMAC-SHA256)**, et les publie vers Apache Kafka. L'infrastructure complète est provisionnée via **Terraform** sur un cluster Kubernetes local **Minikube**.

```
Données CSV Kaggle
      │
      ▼
Producer Python ──► Apache Kafka ──► Apache Flink ──► MinIO (S3)
(Fernet encrypt)    (Strimzi KRaft)  (Session Cluster) (rt-payments)
```

**Stack technique :**

| Composant | Technologie | Version |
|-----------|-------------|---------|
| Orchestration K8s | Minikube | ≥ 1.30 |
| Infrastructure as Code | Terraform | ≥ 1.0 |
| Message broker | Apache Kafka (Strimzi) | 4.1.0 |
| Opérateur Kafka | Strimzi | 0.51.0 |
| Moteur de traitement | Apache Flink | 1.19.1 |
| Stockage objet | MinIO | Standalone |
| Producteur | Python | 3.12 |

---

## 2. Architecture

### Diagramme de flux de données

```mermaid
flowchart LR
    subgraph Local["Machine locale"]
        CSV["📄 creditcard.csv\n(Kaggle dataset)"]
        PROD["🐍 producer.py\nFernet encrypt\nCDC envelope"]
    end

    subgraph K8s["Minikube Cluster"]
        subgraph NS_KAFKA["namespace: kafka"]
            STRIMZI["⚙️ strimzi-cluster-operator\nv0.51.0"]
            BROKER["📦 hps-cluster-dual-role-0\nKafka 4.1.0 (KRaft)\ncontroller + broker"]
            EO["🔧 hps-cluster-entity-operator\ntopic-operator / user-operator"]
            T1["📨 topic: payments\n3 partitions — rétention 24h"]
            T2["📨 topic: payments.dlq\n3 partitions — rétention 72h"]
        end

        subgraph NS_FLINK["namespace: flink"]
            JM["🖥️ flink-jobmanager\nFlink 1.19.1"]
            TM["⚙️ flink-taskmanager\nFlink 1.19.1\n2 slots"]
        end

        subgraph NS_MINIO["namespace: minio"]
            MINIO["🗄️ MinIO Standalone\nbucket: rt-payments"]
        end
    end

    CSV --> PROD
    PROD -->|"port-forward\nlocalhost:9094"| BROKER
    STRIMZI --> BROKER
    STRIMZI --> EO
    EO --> T1
    EO --> T2
    BROKER --> T1
    BROKER --> T2
    T1 -->|"9092 (interne)"| JM
    JM <--> TM
    TM -->|"9000 (interne)"| MINIO
```

### Diagramme d'infrastructure Terraform

```mermaid
graph TD
    TF_MAIN["terraform/main.tf\nProviders: kubernetes, helm, null"]

    TF_NS["namespaces.tf\nkafernetes_namespace\nkafka / flink / minio"]

    TF_KAFKA["kafka.tf\nhelm_release: strimzi-kafka-operator\nnull_resource: kafka_cluster\n(KafkaNodePool + Kafka + KafkaTopic)"]

    TF_MINIO["minio.tf\nhelm_release: minio\nbucket: rt-payments"]

    TF_FLINK["flink.tf\nnull_resource: flink_cluster\n(ServiceAccount + ConfigMap\n+ Deployments + Service)"]

    TF_MAIN --> TF_NS
    TF_NS --> TF_KAFKA
    TF_NS --> TF_MINIO
    TF_NS --> TF_FLINK
    TF_KAFKA -->|"depends_on"| TF_FLINK
```

### Diagramme de sécurité réseau

```mermaid
flowchart TB
    subgraph flink_ns["namespace: flink"]
        JM2["flink-jobmanager"]
        TM2["flink-taskmanager"]
    end

    subgraph kafka_ns["namespace: kafka"]
        KB["Kafka broker\n:9092"]
    end

    subgraph minio_ns["namespace: minio"]
        MN["MinIO\n:9000"]
    end

    JM2 -->|"✅ allow-flink-to-kafka\n(port 9092)"| KB
    TM2 -->|"✅ allow-flink-to-minio\n(port 9000)"| MN
    X1["❌ Tout autre trafic\nentrée bloqué\n(default-deny-ingress)"] -.-> kafka_ns
    X1 -.-> flink_ns
    X1 -.-> minio_ns
```

---

## 3. Prérequis

### Outils requis

| Outil | Version minimale | Installation |
|-------|-----------------|--------------|
| Minikube | ≥ 1.30 | [minikube.sigs.k8s.io](https://minikube.sigs.k8s.io) |
| Helm | ≥ 3.12 | `snap install helm --classic` |
| Terraform | ≥ 1.0 | `snap install terraform` |
| Python | ≥ 3.10 | Système |
| Docker | ≥ 24.0 | Requis comme driver Minikube |

### ⚠️ Note importante sur kubectl

Sur cette machine, **`kubectl` provoque une erreur de segmentation (segfault)**. Il faut **toujours** utiliser l'alias fourni par Minikube :

```bash
# ✅ Commande correcte sur cette machine
minikube kubectl -- get pods -n kafka

# ❌ Ne pas utiliser — provoque un segfault
kubectl get pods -n kafka
```

### Démarrage de Minikube

```bash
minikube start --cpus=2 --memory=3072 --driver=docker
```

> **Contraintes de ressources** : Le cluster est dimensionné pour 2 vCPU et 3 072 Mo de RAM.
> Ces limites ont dicté les choix de configuration mémoire de Flink (voir section [Dépannage](#10-dépannage--problèmes-rencontrés)).

---

## 4. Structure du projet

```
hps-rt-poc/
├── .gitignore                    # Exclut .claude/, .terraform/, tfstate, __pycache__
├── README.md                     # Ce document
├── pyrightconfig.json            # Configuration Pylance/Pyright
├── k8s/
│   ├── flink-rbac.yaml           # ClusterRoleBinding: flink → ClusterRole/edit
│   └── network-policies.yaml    # NetworkPolicies inter-namespaces
├── scripts/
│   ├── producer.py               # Producteur Kafka (Fernet + CDC envelope)
│   └── deploy_infrastructure.sh # Script de déploiement complet end-to-end
└── terraform/
    ├── main.tf                   # Configuration des providers (kubernetes, helm, null)
    ├── namespaces.tf             # Création des namespaces K8s
    ├── variables.tf              # Variables avec valeurs par défaut
    ├── outputs.tf                # Endpoints de sortie (bootstrap, minio, flink REST)
    ├── kafka.tf                  # Strimzi operator + KafkaNodePool + Kafka + KafkaTopic
    ├── minio.tf                  # Helm release MinIO
    └── flink.tf                  # Session cluster Flink (JM + TM + ConfigMap)
```

---

## 5. Composants déployés

### 5.1 Apache Kafka — namespace `kafka`

| Ressource | Nom | Détail |
|-----------|-----|--------|
| Helm release | `strimzi-kafka-operator` | Opérateur Strimzi v0.51.0 |
| Pod opérateur | `strimzi-cluster-operator-*` | Surveille le namespace `kafka` |
| KafkaNodePool | `dual-role` | 1 réplique, rôles : controller + broker |
| Pod broker | `hps-cluster-dual-role-0` | Image `quay.io/strimzi/kafka:0.51.0-kafka-4.1.0` |
| Entity Operator | `hps-cluster-entity-operator` | Topic Operator + User Operator |

**Configuration du cluster Kafka (`hps-cluster`) :**
- Mode : **KRaft** (sans Zookeeper) — activé via `strimzi.io/kraft: enabled`
- Version : **4.1.0**, metadataVersion : **4.1-IV1**
- Listener : `plain` sur le port **9092** (interne, sans TLS)
- Stockage : **éphémère** (PoC, pas de persistence)
- Bootstrap interne : `hps-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092`

**Topics :**

| Nom | Partitions | Réplication | Rétention | Usage |
|-----|-----------|-------------|-----------|-------|
| `payments` | 3 | 1 | 24h (86 400 000 ms) | Transactions chiffrées |
| `payments.dlq` | 3 | 1 | 72h (259 200 000 ms) | Dead Letter Queue |

### 5.2 Apache Flink — namespace `flink`

| Ressource | Nom | Détail |
|-----------|-----|--------|
| ServiceAccount | `flink` | Identité K8s des pods Flink |
| ConfigMap | `flink-config` | Configuration hiérarchique YAML (Flink 1.19) |
| Deployment | `flink-jobmanager` | 1 réplique, REST UI sur :8081 |
| Deployment | `flink-taskmanager` | 1 réplique, 2 slots de tâches |
| Service | `flink-jobmanager` | ClusterIP, ports : 6123 (RPC), 6124 (blob), 8081 (REST) |

**Configuration mémoire Flink (adaptée aux contraintes Minikube) :**

```yaml
jobmanager:
  memory:
    process.size: 512m
    jvm-metaspace.size: 96m      # réduit depuis 256m par défaut
    jvm-overhead.min: 32m        # réduit depuis 192m par défaut
    jvm-overhead.max: 128m
    jvm-overhead.fraction: 0.1

taskmanager:
  memory:
    process.size: 512m
    jvm-metaspace.size: 96m
    jvm-overhead.min: 32m
    framework.heap.size: 64m     # réduit depuis 128m par défaut
    framework.off-heap.size: 64m # réduit depuis 128m par défaut
  numberOfTaskSlots: 2
```

**Endpoint REST (accès local) :**
```bash
minikube kubectl -- port-forward svc/flink-jobmanager 8081:8081 -n flink
# Ouvrir : http://localhost:8081
```

### 5.3 MinIO — namespace `minio`

| Paramètre | Valeur |
|-----------|--------|
| Mode | Standalone |
| Bucket | `rt-payments` |
| Persistence | Désactivée (PoC) |
| Endpoint interne | `http://minio.minio.svc.cluster.local:9000` |
| Credentials | `admin` / `admin123` |

**Accès à la console MinIO :**
```bash
minikube kubectl -- port-forward svc/minio 9001:9001 -n minio
# Ouvrir : http://localhost:9001
```

---

## 6. Déploiement

### 6.1 Déploiement automatique (recommandé)

```bash
cd ~/hps-rt-poc
bash scripts/deploy_infrastructure.sh
```

Ce script :
1. Vérifie les prérequis (minikube en cours d'exécution, helm, terraform)
2. Ajoute les dépôts Helm : `strimzi` et `minio`
3. Exécute `terraform init -upgrade` puis `terraform apply -auto-approve`
4. Applique les manifestes K8s : RBAC et NetworkPolicies
5. Attend que tous les pods soient `Ready`
6. Affiche l'état final et les outputs Terraform

### 6.2 Déploiement manuel étape par étape

```bash
# 1. Démarrer Minikube
minikube start --cpus=2 --memory=3072 --driver=docker

# 2. Ajouter les dépôts Helm
helm repo add strimzi https://strimzi.io/charts/
helm repo add minio   https://charts.min.io/
helm repo update

# 3. Déployer l'infrastructure avec Terraform
cd ~/hps-rt-poc/terraform
terraform init -upgrade
terraform apply -auto-approve

# 4. Appliquer les manifestes K8s
minikube kubectl -- apply -f ~/hps-rt-poc/k8s/flink-rbac.yaml
minikube kubectl -- apply -f ~/hps-rt-poc/k8s/network-policies.yaml
```

### 6.3 Outputs Terraform

Après un `terraform apply` réussi :

```bash
cd ~/hps-rt-poc/terraform && terraform output
```

```
flink_namespace      = "flink"
flink_rest_endpoint  = "http://flink-jobmanager.flink.svc.cluster.local:8081"
kafka_bootstrap      = "hps-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
kafka_namespace      = "kafka"
minio_bucket         = "rt-payments"
minio_endpoint       = "http://minio.minio.svc.cluster.local:9000"
minio_namespace      = "minio"
```

---

## 7. Vérification de l'état du cluster

```bash
# État des pods par namespace
minikube kubectl -- get pods -n kafka
minikube kubectl -- get pods -n flink
minikube kubectl -- get pods -n minio

# État des topics Kafka
minikube kubectl -- get kafkatopic -n kafka

# Tous les namespaces
minikube kubectl -- get namespaces

# NetworkPolicies actives
minikube kubectl -- get networkpolicy -n kafka
minikube kubectl -- get networkpolicy -n flink
minikube kubectl -- get networkpolicy -n minio

# RBAC Flink
minikube kubectl -- get clusterrolebinding flink-role-binding
```

**État attendu nominal :**

```
namespace: kafka
NAME                                           READY   STATUS    RESTARTS
hps-cluster-dual-role-0                        1/1     Running   ...
hps-cluster-entity-operator-*                  2/2     Running   ...
strimzi-cluster-operator-*                     1/1     Running   ...

namespace: flink
NAME                                 READY   STATUS    RESTARTS
flink-jobmanager-*                   1/1     Running   ...
flink-taskmanager-*                  1/1     Running   ...

namespace: minio
NAME                     READY   STATUS    RESTARTS
minio-*                  1/1     Running   ...

kafkatopics (namespace: kafka)
NAME           CLUSTER       PARTITIONS   REPLICATION FACTOR   READY
payments       hps-cluster   3            1                    True
payments.dlq   hps-cluster   3            1                    True
```

---

## 8. Exécution du producteur

### 8.1 Installation des dépendances

```bash
pip install kafka-python cryptography pandas
```

### 8.2 Port-forward Kafka (accès depuis la machine locale)

```bash
# Expose le broker Kafka sur le port local 9094
minikube kubectl -- port-forward svc/hps-cluster-kafka-bootstrap 9094:9092 -n kafka &
```

### 8.3 Lancement du producteur

```bash
python scripts/producer.py \
  --csv /chemin/vers/creditcard.csv \
  --bootstrap localhost:9094 \
  --topic payments \
  --rate 20 \
  --limit 5000
```

**Paramètres :**

| Argument | Valeur par défaut | Description |
|----------|------------------|-------------|
| `--csv` | *(requis)* | Chemin vers le fichier CSV Kaggle |
| `--bootstrap` | `localhost:9094` | Adresse du broker Kafka |
| `--topic` | `payments` | Topic de destination |
| `--rate` | `10.0` | Messages par seconde (doit être > 0) |
| `--limit` | `0` (toutes les lignes) | Nombre maximum de lignes à envoyer |

### 8.4 Format des messages publiés

Chaque message Kafka est un objet JSON structuré en **enveloppe CDC** :

```json
{
  "eventId":   "550e8400-e29b-41d4-a716-446655440000",
  "table":     "creditcard_transactions",
  "operation": "INSERT",
  "payload":   "gAAAAABk...==",
  "timestamp": "2026-04-20T14:30:00.123456+00:00"
}
```

- **`eventId`** : UUID v4 unique par message
- **`table`** : Nom de la table source (CDC)
- **`operation`** : Type d'opération (`INSERT`)
- **`payload`** : Ligne CSV chiffrée avec **Fernet** (AES-128-CBC + HMAC-SHA256)
- **`timestamp`** : Horodatage UTC ISO 8601

> **Important** : La clé Fernet est générée à chaque démarrage et affichée en console.
> Il faut la sauvegarder pour pouvoir déchiffrer les données ultérieurement.
> ```
> [producer] Fernet key (save to decrypt): b'abcDEF123...='
> ```

---

## 9. Sécurité réseau et RBAC

### 9.1 NetworkPolicies

Les NetworkPolicies appliquées suivent le principe du **moindre privilège** :

| Politique | Namespace | Effet |
|-----------|-----------|-------|
| `default-deny-ingress` | `kafka`, `flink`, `minio` | Bloque tout trafic entrant par défaut |
| `allow-flink-to-kafka` | `kafka` | Autorise le namespace `flink` → port 9092 |
| `allow-kafka-internal` | `kafka` | Autorise le trafic intra-namespace (opérateur ↔ broker) |
| `allow-flink-to-minio` | `minio` | Autorise le namespace `flink` → port 9000 |

### 9.2 RBAC Flink

Le ServiceAccount `flink` (namespace `flink`) est lié au ClusterRole `edit` via
le `ClusterRoleBinding` nommé `flink-role-binding`. Cela permet aux jobs Flink
d'interagir avec l'API Kubernetes (découverte de jobs, checkpoints, HA).

---

## 10. Dépannage — Problèmes rencontrés

### 10.1 `kubectl` provoque un segfault

**Symptôme :**
```
Segmentation fault (core dumped)
```

**Cause :** Incompatibilité entre la version de `kubectl` système (v1.29) et le serveur Kubernetes Minikube (v1.35).

**Solution :** Utiliser exclusivement l'alias Minikube :
```bash
# Remplacer toute occurrence de "kubectl" par :
minikube kubectl --
```

---

### 10.2 Conflit de rolebindings Strimzi

**Symptôme :**
```
Error: INSTALLATION FAILED: rendered manifests contain a resource
that already exists. Resource: ClusterRoleBinding ...
```

**Cause :** Une ancienne installation de Strimzi laisse des `ClusterRoleBinding` qui entrent en conflit avec la nouvelle.

**Solution :**
```bash
# Supprimer les ClusterRoleBindings orphelins avant de réappliquer
minikube kubectl -- delete clusterrolebinding \
  strimzi-cluster-operator strimzi-cluster-operator-kafka-broker-delegation \
  --ignore-not-found

# Puis relancer
terraform apply -auto-approve
```

---

### 10.3 CRD `kafkatopics.kafka.strimzi.io` non installé

**Symptôme :**
```
error: the server doesn't have a resource type "kafkatopic"
```

**Cause :** Helm n'installe pas automatiquement les CRDs lors d'un `helm upgrade`. La CRD `kafkatopics` peut être absente après une réinstallation.

**Solution :**
```bash
# Extraire et appliquer toutes les CRDs Strimzi depuis le chart
helm show crds strimzi/strimzi-kafka-operator --version 0.51.0 \
  > /tmp/strimzi-crds.yaml
minikube kubectl -- apply -f /tmp/strimzi-crds.yaml
```

---

### 10.4 Namespace `kafka` bloqué en état `Terminating`

**Symptôme :**
```
minikube kubectl -- get namespace kafka
NAME    STATUS        AGE
kafka   Terminating   10m
```

**Cause :** Des finalizers Strimzi sur les ressources `KafkaTopic` empêchent la suppression du namespace après la désinstallation de l'opérateur.

**Solution :**
```bash
# Forcer la suppression des finalizers du namespace
minikube kubectl -- replace --raw \
  "/api/v1/namespaces/kafka/finalize" \
  -f - <<EOF
{
  "apiVersion": "v1",
  "kind": "Namespace",
  "metadata": {"name": "kafka"},
  "spec": {"finalizers": []}
}
EOF
```

---

### 10.5 Flink crash — `Total Flink Memory < Off-heap Memory`

**Symptôme :**
```
IllegalConfigurationException: Total Flink Memory (64MB) < Off-heap Memory (128MB)
```

**Cause :** Avec `jobmanager.memory.process.size: 512m`, les valeurs par défaut de Flink 1.19 consomment toute la mémoire en overhead JVM :
- JVM Overhead minimum par défaut : **192m**
- JVM Metaspace par défaut : **256m**
- Total overhead : 448m → il ne reste que **64m** pour Flink, insuffisant.

**Solution appliquée dans `flink.tf` :**
```yaml
jobmanager:
  memory:
    process.size: 512m
    jvm-metaspace.size: 96m      # était 256m
    jvm-overhead.min: 32m        # était 192m
    jvm-overhead.max: 128m
    jvm-overhead.fraction: 0.1
```

Calcul résultant :
- JVM overhead effectif : `0.1 × 512 = 51.2m` → dans `[32m, 128m]` ✓
- JVM metaspace : `96m`
- **Mémoire Flink disponible : `512 - 51.2 - 96 = 364.8m`** > 128m minimum ✓

---

### 10.6 Flink crash — ConfigMap monté en lecture seule

**Symptôme :**
```
/opt/flink/bin/config-parser-utils.sh: line 45:
/opt/flink/conf/config.yaml: Read-only file system
```

**Cause :** Flink 1.19 utilise `/opt/flink/conf/config.yaml` (format YAML hiérarchique, non l'ancien `flink-conf.yaml`). Le script de démarrage `config-parser-utils.sh` tente d'**écrire** dans ce fichier au démarrage. Un montage `subPath` depuis un ConfigMap est en lecture seule, ce qui provoque l'échec.

**Solution appliquée — `initContainer` + `emptyDir` :**

```yaml
volumes:
  - name: flink-config-volume
    configMap:
      name: flink-config
  - name: flink-conf-dir
    emptyDir: {}          # volume inscriptible

initContainers:
  - name: copy-config
    image: apache/flink:1.19.1-scala_2.12-java11
    command:
      - sh
      - -c
      - "cp -r /opt/flink/conf/. /tmp/conf/ && cp /tmp/configmap/config.yaml /tmp/conf/config.yaml"
    volumeMounts:
      - name: flink-config-volume
        mountPath: /tmp/configmap
      - name: flink-conf-dir
        mountPath: /tmp/conf

containers:
  - name: jobmanager
    volumeMounts:
      - name: flink-conf-dir
        mountPath: /opt/flink/conf  # répertoire inscriptible
```

L'`initContainer` copie d'abord tous les fichiers originaux de `/opt/flink/conf/` du conteneur vers l'`emptyDir`, puis écrase `config.yaml` avec notre version personnalisée.

---

### 10.7 Terraform — forcer la ré-exécution d'un `null_resource`

**Symptôme :** Les changements apportés à `flink.tf` ou `kafka.tf` ne sont pas pris en compte lors de `terraform apply` car le `null_resource` est déjà dans l'état `created`.

**Solution :**
```bash
cd ~/hps-rt-poc/terraform

# Marquer la ressource comme "à recréer"
terraform taint null_resource.flink_cluster
terraform taint null_resource.kafka_cluster

# Appliquer uniquement la ressource ciblée
terraform apply -target=null_resource.flink_cluster -auto-approve
```

---

## 11. Suppression de l'infrastructure

```bash
# Supprimer toute l'infrastructure Terraform
cd ~/hps-rt-poc/terraform
terraform destroy -auto-approve

# Supprimer les ressources K8s créées hors Terraform
minikube kubectl -- delete clusterrolebinding flink-role-binding --ignore-not-found
minikube kubectl -- delete -f ~/hps-rt-poc/k8s/network-policies.yaml --ignore-not-found

# Optionnel : arrêter Minikube
minikube stop
```

---

*PoC développé dans le cadre d'un stage en Data Engineering — HPS Group, Casablanca.*
