const net = require('net');
const fs = require('fs');
const _ = require('lodash')

let SPECIAL_RUN_MODE = false;

let id;
let port;
let localString;
let originString;

let otherClients = [];
let myActions = [];
let myTimestamp = 0;
let goodbyeCounter = 0;

let otherClientsLatestOperationTimestamp;
let operationsHistory = []; // form of element: { action, updatedString, timestamp, cid}

// ----------------------------- Process input file and prepare client state -----------------------------

const divideStringOn = (s, search) => {
    const index = s.indexOf(search)
    if (index < 0) {
        return [s, ''];
    }
    const first = s.slice(0, index)
    const rest = s.slice(index + search.length)
    return [first, rest]
}

// Read the input file
let filename = process.argv[2];
let data = fs.readFileSync(filename, { encoding: 'utf8' });

let client;     // represents current client part
let others;     // represents other clients part
let actions;    // represents current client local update operations
[client, others, actions] = data.split('\r\n\r\n', 3)

// process current client information
let rest;
[id, rest] = divideStringOn(client, '\r\n');
[port, localString] = divideStringOn(rest, '\r\n');
originString = localString;

// process other clients information
while (others !== '') {
    let otherClient;
    [otherClient, others] = divideStringOn(others, '\r\n');
    const [clientId, clientIP, clientPort] = otherClient.split(' ', 3);
    otherClients.push({ clientId, clientIP, clientPort });
}
goodbyeCounter = otherClients.length + 1;
otherClientsLatestOperationTimestamp = new Array(otherClients.length).fill(0);

// process local operations information
while (actions !== '') {
    let action;
    [action, actions] = divideStringOn(actions, '\r\n');
    let [type, args] = divideStringOn(action, ' ');
    args = args.split(' ');
    switch (type) {
        case 'insert':
            args.length > 1 ?
                myActions.push({ type, character: args[0], index: parseInt(args[1]) }) :
                myActions.push({ type, character: args[0] });
            break;
        case 'delete':
            myActions.push({ type, index: parseInt(args[0]) });
            break;
        default:
            break;
    }
}

// ----------------------------- Helpers functions -----------------------------

const updateTimestamp = (messageTimestamp, eventType) => {
    if (eventType === 'receive') {
        myTimestamp = Math.max(messageTimestamp, myTimestamp);
    }

    myTimestamp++;

    if (eventType === 'send') {
        messageTimestamp = myTimestamp;
    }
}

const removeOperations = ({ action, updatedString, timestamp, cid }) => {
    let shouldRemoveOperation = otherClientsLatestOperationTimestamp.reduce((acc, curr) => acc && (timestamp <= curr), true);

    if (shouldRemoveOperation) {
        let indexOfOperationToRemove = operationsHistory.findIndex((op) => _.isEqual(op, { action, updatedString, timestamp, cid }));
        operationsHistory.splice(indexOfOperationToRemove, 1);
        console.log(`Client ${id} removed operation ${JSON.stringify(action)}, ${timestamp} from storage`)
    }
}

const updateOperationsHistory = (action, updatedString, timestamp, cid) => {
    operationsHistory.push({ action, updatedString, timestamp, cid});
}

const updateGoodbyeCounter = () => {
    goodbyeCounter--;
    if (goodbyeCounter === 0) {
        console.log(`Client ${id} finished his local string modifications`);
        console.log(localString);
        clientsSockets.forEach(clientSocket => clientSocket.end());
        console.log(`Client ${id} is exiting`);
        server.close();
    }
}

const applyMerge = (action, timestamp, cid) => {
    let index = operationsHistory.length - 1;
    while (index >= 0 && operationsHistory[index].timestamp > timestamp) {
        index--;
    }
    if (index >= 0 && operationsHistory[index].timestamp === timestamp) {
        while (index >= 0 && operationsHistory[index].timestamp === timestamp && operationsHistory[index].cid > cid) {
            index--;
        }
    }
    localString = index === -1 ? originString : operationsHistory[index].updatedString;
    console.log(`Client ${id} started merging, from ${index === -1 ? timestamp : operationsHistory[index].timestamp} time stamp, on ${localString}`);
    operationsHistory.splice(index + 1, 0, { action, updatedString: localString, timestamp, cid });

    let i;
    for (i = index + 1; i < operationsHistory.length; i++) {
        applyActionToString(operationsHistory[i].action);
        operationsHistory[i].updatedString = localString;
        console.log(`operation ${JSON.stringify(operationsHistory[i].action)}, ${operationsHistory[i].timestamp}, string: ${operationsHistory[i].updatedString}`)
    }
    console.log(`Client ${id} ended merging with string ${localString}, on timestamp ${operationsHistory[i - 1].timestamp}`)
    removeOperations(operationsHistory[index === -1 ? index + 1 : index]);
}

function getPosition(string, subString, index) {
    return string.split(subString, index).join(subString).length;
}

const handleData = (socket, buffer) => {
    if (buffer.toString().substring(0, 7) === "goodbye") {
        updateGoodbyeCounter();
    }
    else {
        // update strings while buffer still has messages
        while(buffer.length > 0) {
            const parsedMessage = JSON.parse(buffer.toString().substring(0, getPosition(buffer.toString(), "}", 2) + 1));
            const { action, timestamp, cid } = parsedMessage;
            otherClientsLatestOperationTimestamp[cid - 1] = timestamp;
            updateTimestamp(timestamp, 'receive');
            console.log(`Client ${id} received an update operation ${JSON.stringify(action)}, ${timestamp} from client ${cid}`);
            const index = operationsHistory.length - 1;
            if (index >= 0 && 
                (timestamp < operationsHistory[index].timestamp || 
                    (timestamp === operationsHistory[index].timestamp && cid < operationsHistory[index].cid))) {
                applyMerge(action, timestamp, cid);
            }
            else {
                applyActionToString(action);
                updateOperationsHistory(action, localString, timestamp, cid);
            }
            // buffer will start from next action
            buffer = Buffer.from(buffer.toString().substring(getPosition(buffer.toString(), "}", 2) + 1))
        }
    }
}

const applyActionToString = (action) => {
    switch (action.type) {
        case 'insert':
            if (action.index && action.index >= localString.length)
                break;
            localString = action.index ? localString.slice(0, action.index).concat(action.character).concat(localString.slice(action.index))
                : localString.concat(action.character);
            break;
        case 'delete':
            if (action.index < localString.length)
                localString = localString.slice(0, action.index).concat(localString.slice(action.index + 1));
            break;
        default:
            break;
    }
}

// ----------------------------- Server functions -----------------------------

let serversSockets = [];    // all sockets to the servers I connected to
let clientsSockets = [];    // all sockets of clients that connected to me

let server = net.createServer()
    .listen(port, "127.0.0.1", () => {
    })
    .on('connection', socket => {
        clientsSockets.push(socket);

        socket.on('data', buffer => {
            handleData(socket, buffer);
        })
    });

// ----------------------------- Client functions -----------------------------

const connectToServers = () => {
    for (const otherClient of otherClients) {
        if (otherClient.clientId > id) {
            let socket = net.createConnection(otherClient.clientPort, otherClient.clientIP, () => {});
            socket.on('data', buffer => {
                handleData(socket, buffer);
            })
            serversSockets.push(socket);
        }
    }
}

connectToServers();

// wait some time to let other clients connect to me before starting event loop
setTimeout(() => {
    eventLoop().then(() => {
        setTimeout(() => {
            // event loop finished, send goodbye to all the other clients
            updateGoodbyeCounter();
            serversSockets.forEach(serverSocket => serverSocket.write(`goodbye ${id}`));
            clientsSockets.forEach(clientSocket => clientSocket.write(`goodbye ${id}`));
        }, 30000)
    })
}, 10000)

const writeToSockets = async (buf) => {
    serversSockets.forEach(serverSocket => serverSocket.write(JSON.stringify(buf)));
    clientsSockets.forEach(clientSocket => clientSocket.write(JSON.stringify(buf)));
}

const eventLoop = async () => {
    let numOfLocalUpdates = 0;
    let lastUpdates = [];
    let numOfActions = myActions.length;

    const sendMessage = async (action) => {
        applyActionToString(action);
        numOfLocalUpdates++;
        numOfActions--;
        updateTimestamp(myTimestamp, 'send');
        updateOperationsHistory(action, localString, myTimestamp, id);
        otherClientsLatestOperationTimestamp[id - 1] = myTimestamp;
        let buf = { action, timestamp: myTimestamp, cid: id };
        lastUpdates.push(buf);

        if(SPECIAL_RUN_MODE) {
            if (numOfLocalUpdates === 10) {
                lastUpdates.forEach((update) => {
                    setTimeout(() => { writeToSockets(update) }, 3000);
                });
                numOfLocalUpdates = 0;
                lastUpdates = [];
            } else if (numOfActions === 0) {
                lastUpdates.forEach((update) => {
                    setTimeout(() => { writeToSockets(update) }, 3000);
                });
            }
        } else {
            setTimeout(() => { writeToSockets(buf) }, 3000);
        }

    }

    let i = 1;

    for (const action of myActions) {
        setTimeout(() => {
            sendMessage(action)
        }, i * 1000);
        i++;
    }

}

