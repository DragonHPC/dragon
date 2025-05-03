Glossary
========

.. figure:: images/dragon_domain_model.svg
   :scale: 75%
   :name: dragon-domain-model

   **UML diagram of the most important Dragon concepts and their relation. Open arrows are read as "is a", diamond edges as "contains", normal arrows are annotated**

.. glossary::

   to attach
     Synonym for bringing an object in Dragon managed memory into scope of the
     current process by deserializing a :term:`Serialized Descriptor` and thus gaining access
     to the object.

   Channel
     Dragon's communication primitive, which is implemented as a message queue in managed memory
     with a feature rich collection of events. Supports zero-copy for on :term:`Node` transfer
     and arbitrary message sizes via side loading.

   Directed Graph
     A data structure made of knots and arrows that abstracts the control flow of a
     computer program in a general way. *Directed Graphs* are self-similar and can
     describe data movement across multiple systems and heterogeneous network
     environments, if they abstract a hardware description.

   Distributed System
     A *Distributed System* is a collection of *Nodes* connected by a
     homogeneous network - think cluster or supercomputer. Such a *Distributed System*
     has relaxed requirements regarding network security/encryption and
     possibly a performant interconnect.

   Dragon Object
     A *Dragon Object* is an abstraction of one or more :term:`System Resource`
     into a programmable object. High level Dragon objects, such as the
     :any:`dragon.data.ddict.DDict` are composites of
     other *Dragon Objects* to provide convenient functionality to the programmer. Every
     *Dragon Object* can be represented by either :term:`Object Name`,
     :term:`Object UID` or :term:`Serialized Descriptor`. The representation can
     be handed to other processes, who can in turn use the representation to
     gain access to the object and its functionality. See also
     :ref:`the Dragon resource model<uguide/resource_model:The Dragon HPC Resource Model>`.

   Federated System
     A *Federated System* is a collection of :term:`distributed systems <Distributed System>`
     connected by a heterogeneous network - think your laptop, a supercomputer
     and a cloud instance connected via the internet. Such a
     *Federated System* has strict requirements regarding network
     security/encryption and an unreliable interconnect with long latencies and
     possibly even packet loss.

   Group of resources
     A *Group of resources* is a primary resource component that consists of the same or different
     types of primary Dragon objects, such as processes, channels, memory pools and nodes.
     A Group can collectively manage its members.

   Managed Object
     We call a Dragon object managed, if the object can be discovered by :term:`Object Name`
     or :term:`Object UID`, i.e. the Dragon runtime services can be queried for an object's
     :term:`Serialized Descriptor` by any process or thread on a system.

   Node
     A *Node* is the smallest part of a system that can function on its own. We
     would refer to a laptop as a single *Node*. A *Node* is a :term:`System Resource`
     in the Dragon model. Hence it is  abstracted by the `node` :term:`Dragon Object`
     in the Dragon Client API. The abstraction contains information such as the number of
     CPUs or GPUs.

   Object Name
     A unique string given to a :term:`Managed Object` upon creation. Thus it is
     registered with the Dragon runtime. :term:`Managed processes <Managed Object>` may query the
     runtime with the *Object Name* to obtain the :term:`Serialized Descriptor`
     and use/attach to the :term:`Managed Object`.

   Object UID
     Same as :term:`Object Name`, but using a 64-bit integer.

   Policy
     A data structure handed to APIs of the runtime that controls aspects of the
     interfaces and its objects.

   Primary Object
     *Primary objects* are :term:`Dragon objects <Dragon Object>` in the Client API
     that are not composed of other Dragon objects, i.e. are irreducible. At present
     Dragon has four *primary objects*: managed memory, processes, channels and nodes.
     Each *primary object* abstracts a :term:`system resource <System Resource>`:
     memory/disk, compute, network, node capabilities. See also
     :ref:`the Dragon resource model<uguide/resource_model:The Dragon HPC Resource Model>`.

   Refcounted Object
     A *Refcounted Object* is a :term:`Managed Object` that is garbage collected
     by the Dragon runtime services. Dragon's services will maintain a count of
     the number of references to the resource across processes. When the count
     drops to zero the resource will be cleaned up automatically. Reference
     counting of :term:`unmanaged objects <Unmanaged Object>` is invalid, and
     such a request will return an error. See also :ref:`uguide/resource_model:Policies`
     and :any:`dragonPolicy_t`.

   Serialized Descriptor
     A small unique bytes-like object that allows a process to "attach" to a
     Dragon object created by another process. *Serialized Descriptors* can be
     encoded to appear as strings and be passed between processes via stdin.
     They are a fundamental property of Dragon's managed memory implementation.

   System Resource
     A *System Resource* is a property of a system that is abstracted into a
     programmable :term:`Dragon Object` by one or more of the Dragon APIs. The
     four fundamental system resources abstracted by Dragon in the Client API
     are: processes, shared memory, network connectivity and hardware nodes.

   Transparency
     We call a programming model *transparent*, when its objects can be used independently
     of process location on a :term:`Distributed System` or :term:`Federated System`.
     For a more precise definition see `Arjona et al. 2022 <https://arxiv.org/abs/2205.08818>`_.

   Unmanaged Object
     We call a Dragon object *unmanaged*, if the user program cannot obtain the
     object's :term:`Serialized Descriptor` from the Dragon runtime services.
     Its life-cycle must be completely managed by user process(es), *Unmanaged
     Objects* are never :term:`refcounted <Refcounted Object>`. Processes can
     only gain access to the resource through explicit communication of a
     serialized descriptor of the resource.

   Workflow
     A *Workflow* is a program describing the movement and processing of data
     across a :term:`Federated System`. Workflows can be conveniently abstracted by
     :term:`directed graphs <Directed Graph>`. Dragon will enable workflow programming by providing a
     directed graph :term:`Dragon Object` in the Dragon Native API.


