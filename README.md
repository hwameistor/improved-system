# HwameiStor Reliable Helper System

## Introduction

The Reliable-helper-system is one of subsidiary components of the HwameiStor. It provides the reliability maintenance functionality such as replacedisk which support after migrating the data on the hard disk, to perform the hard disk replacement operation .

Reliable-helper-system will work for LVM volumes now. At present, the RHS project is still in the alpha stage.

## Architecture of HwameiStor Reliable Helper System

![image](https://github.com/hwameistor/reliable-helper-system/blob/main/doc/design/HwameiStor-arch.png)

## Concepts

**ReplaceDisk(RD)**: A `RD` resource object represents one replace disk task on the host.

**LocalVolumeMigrate(LVM)**: The way to migrate and backup replaced disk data to disks of other nodes.

## Usage
If you want to entirely deploy HwameiStor, please refer to [here](https://github.com/hwameistor/helm-charts). If you just want to deploy RHS separately, you can refer to the following installation steps.

## Install Reliable Helper System

### 1. Clone this repo to your machine:
```
# git clone https://github.com/hwameistor/reliable-helper-system.git
```

### 2. Change to deploy directory:
```
# cd deploy
```

### 3. Deploy CRDs and run reliable-helper-system

#### 3.1 Deploy RHS CRDs
```
# kubectl apply -f deploy/crds/
```

#### 3.2 Deploy RBAC CRs and operators
```
# kubectl apply -f deploy/
```

### 4. Get ReplaceDisk Infomation
```
kubectl  get replacedisk
NAME                       AGE
replacedisk-sample-node1   21h

```

`kuebctl get replacedisk <name> -o yaml` View more information about replacedisk.

## Feedbacks

Please submit any feedback and issue at: [Issues](https://github.com/hwameistor/reliable-helper-system/issues)
