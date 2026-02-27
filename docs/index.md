# orrp

Hello! Welcome to the **orrp** Documentation site. We're happy to have you here. 

orrp is a high-performing database that is built for heavy write workloads like analytics, event stores, IoT workloads, network traffic, etc. It is eventually consistent, which means that writes are not always immediately available for queries.

Everything that you write to orrp is an event. An event is a piece of data that is composed of key-value tags and is associated with an entity. An entity might be a user, a node, a service, a device, etc. These events get written to the orrp database and can be queried using a SQL-like syntax that supports range queries, pagination, indexing, and more. 

orrp is also built for scale. Under the hood, it uses libuv, which is the same event loop that powers node.js. This allows orrp to handle many connections concurrently. It also runs in a multi-threaded environment, utilizing lock-free data structures and other advanced techniques to ensure that it can handle high load quickly. 

Feel free to poke around and read some of the other documentation on this site. It's a work in progress, and more documentation will be added over time. As always, if you have any questions or ideas (or want to contribute!), start a new discussion or open an Issue in the [orrp GitHub repository](https://github.com/MatthewFay/orrp).

**orrp is completely free to use and open-source. Apache 2.0 Licensed.**

<!-- ## Project layout

    mkdocs.yml    # The configuration file.
    docs/
        index.md  # The documentation homepage.
        ...       # Other markdown pages, images and other files. -->
