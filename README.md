# fantasm
Automatically exported from code.google.com/p/fantasm

A taskqueue-backed, configuration-based Finite State Machine for Google App Engine Python

A Python library for a configuration-based finite state machine workflow implementation based on taskqueue.

Fantasm allows developers to create finite state machines using YAML markup. Each state and transition can have an action, implemented by a Python class that you write. Transitions between states are all performed using queues task (i.e., the taskqueue API) for high resiliency and operational visibility.

In more advanced implementations, you can configure your machine to "fan-out," that is, create many replicas of a given machine to massively scale over large datasets. Additionally, you can configure these replicated machines to "fan-in" allowing you to join the machines back together, typically to facilitate better batch processing.

Fantasm is designed to allow developers to take advantage of the scale-out aspects of App Engine and the high resiliency of the taskqueue API without needing much thought about the boilerplate code to make this happen.

Visit GettingStarted to get started immediately.
