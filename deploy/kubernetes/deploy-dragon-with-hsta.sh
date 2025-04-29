#!/bin/bash

# generate a unique job label
FRONTEND_JOB_LABEL="dragon-fe-$(date +%Y%m%d%H%M%S)"


# Store the ConfigMap for the backend pods

# generate a unique job label
BACKEND_JOB_LABEL="dragon-be-$(date +%Y%m%d%H%M%S)"

# Define the Job YAML configuration
cat <<EOF | rancher kubectl apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: backend-job-config-${FRONTEND_JOB_LABEL}
  namespace: default
data:
  backend_pod_${FRONTEND_JOB_LABEL}.yml: |
    apiVersion: batch/v1
    kind: Job
    metadata:
      name: ${BACKEND_JOB_LABEL}
      namespace: default
      ownerReferences:
      - apiVersion: batch/v1
        controller: true
        kind: Job
        name: ${FRONTEND_JOB_LABEL}
        uid: "{{parentDeployment.metadata.uid}}"
    spec:
      ttlSecondsAfterFinished: 100 # seconds
      completions: 2
      parallelism: 2  # Number of pods to run concurrently
      backoffLimit: 0 # Make sure that the pod does not restart when errored
      template:
        metadata:
          labels:
            app: ${BACKEND_JOB_LABEL}
            tier: backend
        spec:
          serviceAccountName: backend-pod-service-account
          affinity:
            podAntiAffinity:
                requiredDuringSchedulingIgnoredDuringExecution:
                - labelSelector:
                    matchLabels:
                      tier: frontend
                  topologyKey: kubernetes.io/hostname
          topologySpreadConstraints:
          - maxSkew: 1
            topologyKey: kubernetes.io/hostname
            whenUnsatisfiable: DoNotSchedule
            labelSelector:
              matchLabels:
                tier: backend
          containers:
          - name: backend
            image: mkalantzi/dragondev:latest
            volumeMounts:
            - name: dragon-develop-volume
              mountPath: /dragon-develop
            - name: dragonshm
              mountPath: /dev/shm
            - mountPath: /run/nvidia/driver
              name: nvidia-drivers
              mountPropagation: HostToContainer
            env:
            - name: BACKEND_OVERLAY_PORT
              value: "8081"
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: POD_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
            - name: POD_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
            - name: BACKEND_JOB_LABEL
              value: ${BACKEND_JOB_LABEL}
            - name: POD_UID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.uid
            - name: DRAGON_LOG_DEVICE_ACTOR_FILE
              value: "DEBUG"
            # - name: UCX_TCP_MAX_CONN_RETRIES
            #   value: "250"
            # - name: UCX_LOG_LEVEL
            #   value: "DEBUG"
            # - name: _DRAGON_ENABLE_HUGEPAGES
            #   value: "0"
            # - name: DRAGON_HSTA_UCX_NO_MEM_REGISTER
            #   value: "1"
            - name: DRAGON_FE_SDESC
              value: "{{temp_fe_sdesc}}"
            - name: FE_LABEL_SELECTOR
              value: "{{temp_fe_label}}"
            - name: "{{DRAGON_FE_GW}}"
              value: "{{temp_fe_gw}}"
            - name: DRAGON_TELEMETRY_LEVEL
              value: "{{temp_telemetry_level}}"
            ports:
            - containerPort: 8081
            - containerPort: 4242
            command: ['/bin/bash', '-c']
            args:
            - |
              ucx_info -v;
              cd /dragon-develop/hpc-pe-dragon-dragon-k8s-merge-develop;
              . hack/setup;
              ldconfig -p | grep ucp;
              ldd /dragon-develop/hpc-pe-dragon-dragon-k8s-merge-develop/src/lib/libdfabric_ucx.so;
              ulimit -l;
              ulimit -l unlimited;
              ulimit -l;
              dragon-backend;
            securityContext:
              allowPrivilegeEscalation: true
              capabilities:
                add:
                - IPC_LOCK
                - IPC_OWNER
                - SYS_RESOURCE
              privileged: true
              readOnlyRootFilesystem: false
              runAsNonRoot: false
            memory:
              hugepages:
                pageSize: 1Gi
            resources:
              requests:
                # cpu: 200m # 20% of one full CPU core
                # memory: 128Mi
                nvidia.com/gpu: 2
              limits:
                nvidia.com/gpu: 2
          tolerations:
            - key: node-role.kubernetes.io/compute
              operator: Exists
              effect: NoExecute
            # - key: dragonhpc/compute
            #   operator: Exists
            #   effect: NoExecute
            - key: nvidia.com/gpu.present
              operator: Exists
              effect: NoSchedule
          nodeSelector:
            node-role.kubernetes.io/compute: "true"
          volumes:
          - name: dragon-develop-volume
            persistentVolumeClaim:
              claimName: dragon-develop-pvc
          - name: dragonshm
            emptyDir:
              medium: Memory
              sizeLimit: 64Gi
          - name: nvidia-drivers
            hostPath:
              path: /run/nvidia/driver
              type: Directory
          restartPolicy: Never
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  namespace: default
  name: backend-role
rules:
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "list", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: backend-role-binding
  namespace: default
subjects:
- kind: ServiceAccount
  name: backend-pod-service-account
  namespace: default
roleRef:
  kind: Role
  name: backend-role
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: backend-pod-service-account
  namespace: default
---
apiVersion: v1
kind: Service
metadata:
  name: backend-pods-service
spec:
  ports:
    - protocol: TCP
      port: 8888
      targetPort: 8888
  selector:
    node_index: ${BACKEND_JOB_LABEL}_pod_0
---
apiVersion: v1
kind: Service
metadata:
  name: telemetry-service
spec:
  ports:
    - protocol: TCP
      port: 4242
      targetPort: 4242
  selector:
    telemetry: ${BACKEND_JOB_LABEL}_aggregator
EOF
echo "Job for the Backends with label ${BACKEND_JOB_LABEL} created successfully."




# Deploy the frontend pod

# Define the Job YAML configuration
cat <<EOF | rancher kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${FRONTEND_JOB_LABEL}
  namespace: default
spec:
  ttlSecondsAfterFinished: 100 # seconds
  completions: 1
  parallelism: 1
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: ${FRONTEND_JOB_LABEL}
        tier: frontend
    spec:
      serviceAccountName: frontend-pod-service-account
      restartPolicy: Never
      containers:
      - name: frontend
        image: mkalantzi/dragondev:latest
        volumeMounts:
        - name: dragon-develop-volume
          mountPath: /dragon-develop
        - name: backend-job
          mountPath: /config
        - name: dragonshm
          mountPath: /dev/shm
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
        - name: POD_IP
          valueFrom:
            fieldRef:
              fieldPath: status.podIP
        - name: POD_UID
          valueFrom:
            fieldRef:
              fieldPath: metadata.uid
        - name: FRONTEND_JOB_LABEL
          value: ${FRONTEND_JOB_LABEL}
        - name: FRONTEND_PORT
          value: "8080"
        command: ['/bin/bash', '-c']
        args:
        - |
          cd /dragon-develop/hpc-pe-dragon-dragon-k8s-merge-develop;
          . hack/setup;
          # ldconfig -p | grep ucp;
          # ldd /dragon-develop/hpc-pe-dragon-dragon-k8s-merge-develop/src/lib/libdfabric_ucx.so;
          # dragon -l actor_file=DEBUG -t hsta examples/multiprocessing/p2p_lat.py --dragon;
          # dragon -t hsta examples/multiprocessing/p2p_lat.py --dragon;
          dragon -l actor_file=DEBUG -t hsta --telemetry-level=3 examples/jupyter/start_jupyter.py;
          # dragon -t hsta --telemetry-level=2 examples/dragon_telemetry/scipy_scale_work.py --dragon --iterations 1000 --burns 1 --work_time 1;
          # dragon -t hsta --telemetry-level=3 examples/dragon_telemetry/scipy_scale_work.py --dragon;
          # dragon simple_gpu_test.py;
          # dragon --telemetry-level=3 simple_gpu_test.py;
          # dragon -t hsta examples/multiprocessing/p2p_bw.py --iterations 1000 --lg_max_message_size 21 --dragon;
          # dragon test/multi-node/test_barrier.py -v -f;
          # dragon examples/multiprocessing/numpy-mpi4py-examples/scipy_scale_work.py --dragon --num_workers 354 --mem 1073741824 --size 256 --iterations 2 --burns 0 --work_time 4;
          # dragon examples/benchmarks/gups_ddict.py --num_nodes=4;
          # dragon examples/benchmarks/gups_ddict.py --num_nodes=6 \
          #            --nclients=2048 \
          #            --managers_per_node=4 \
          #            --total_mem_size=12 \
          #            --mem_frac=0.5 \
          #            --iterations=1;
          rm -f *${BACKEND_JOB_LABEL}*.log;
        ports:
        - containerPort: 8080
      nodeSelector:
        kubernetes.io/hostname: svc-01
      tolerations:
        - key: node-role.kubernetes.io/compute
          operator: Exists
          effect: NoExecute
      volumes:
      - name: backend-job
        configMap:
          name: backend-job-config-${FRONTEND_JOB_LABEL}
      - name: dragon-develop-volume
        persistentVolumeClaim:
          claimName: dragon-develop-pvc
      - name: dragonshm
        emptyDir:
          medium: Memory
          sizeLimit: 16Gi
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: frontend-pod-service-account
  namespace: default
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  namespace: default
  name: frontend-role
rules:
- apiGroups: [""]
  resources: ["pods", "pods/log"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["batch"]
  resources: ["jobs"]
  verbs: ["get", "list", "create", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: frontend-role-binding
  namespace: default
subjects:
- kind: ServiceAccount
  name: frontend-pod-service-account
  namespace: default
roleRef:
  kind: Role
  name: frontend-role
  apiGroup: rbac.authorization.k8s.io
EOF
echo "Job for the Frontend with label ${FRONTEND_JOB_LABEL} created successfully."


# Wait for the Job to complete
echo "Waiting for the Frontend Job to complete and delete the Backend ConfigMap."
while true; do
  if ! rancher kubectl get job ${FRONTEND_JOB_LABEL} &>/dev/null; then
    echo "Job ${FRONTEND_JOB_LABEL} not found or deleted."

    echo "Delete the dragon log files."
    POD_NAME=$(rancher kubectl get pods -l job-name=${FRONTEND_JOB_LABEL} -o jsonpath='{.items[0].metadata.name}')
    rancher kubectl exec -it $POD_NAME -- sh -c 'cd /dragon-develop/hpc-pe-dragon-dragon-k8s-merge-develop && rm *${BACKEND_JOB_LABEL}*.log'

    echo "Delete the frontend pod if still exists."
    rancher kubectl delete pods --selector=job-name=${FRONTEND_JOB_LABEL}
    if [ $? -ne 0 ]; then
        echo "Failed to delete the frontend Pod."
        exit 1
    fi
    echo "The frontend Pod is deleted successfully."

    echo "Delete the backend pods if still exist."
    rancher kubectl delete pods --selector=job-name=${BACKEND_JOB_LABEL}
    if [ $? -ne 0 ]; then
        echo "Failed to delete the backend Pods."
        exit 1
    fi
    echo "The backend Pods are deleted successfully."

    break
  fi
  FE_JOB_STATUS=$(rancher kubectl get job ${FRONTEND_JOB_LABEL} -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}')
  if [ "${FE_JOB_STATUS}" == "True" ]; then
    echo "Frontend Job completed successfully."
    break
  fi
  sleep 10
done

echo "Now, clean up the Backend ConfigMap."
rancher kubectl delete configmap backend-job-config-${FRONTEND_JOB_LABEL}
if [ $? -ne 0 ]; then
  echo "Failed to clean up the Backend ConfigMap."
  exit 1
fi
echo "Cleanup completed."