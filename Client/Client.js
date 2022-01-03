const net = require('net');
const fs = require('fs');

let SPECIAL_RUN_MODE = false;

let id;
let port;
let localString;
let originString;

let otherClients = [];
let myActions = [];
let myTimestamp = 0;
let goodbyeCounter = 0;

let operationsHistory = [];

const divideStringOn = (s, search) => {
    const index = s.indexOf(search)
    if (index < 0) {
        return [s, ''];
    }
    const first = s.slice(0, index)
    const rest = s.slice(index + search.length)
    return [first, rest]
}

// Read the file and create client.
let filename = process.argv[2];
let data = fs.readFileSync(filename, { encoding: 'utf8' });

let client;
let others;
let actions;
[client, others, actions] = data.split('\r\n\r\n', 3)
let rest;
[id, rest] = divideStringOn(client, '\r\n');
[port, localString] = divideStringOn(rest, '\r\n');
originString = localString;
while (others !== '') {
    let otherClient;
    [otherClient, others] = divideStringOn(others, '\r\n');
    const [clientId, clientIP, clientPort] = otherClient.split(' ', 3);
    otherClients.push({ clientId, clientIP, clientPort });
}
goodbyeCounter = otherClients.length + 1;
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

const updateTimestamp = (messageTimestamp, eventType) => {
    if (eventType === 'receive') {
        myTimestamp = Math.max(messageTimestamp, myTimestamp);
    }

    myTimestamp++;

    if (eventType === 'send') {
        messageTimestamp = myTimestamp;
    }
}

//TODO: implement
const removeOperations = () => {
    let allClients = Array(otherClients.length).fill(false);
    let numOfAllClients = otherClients.length;

    console.log(`Client ${id} removed operation <operation, timestamp> from storage`)
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

    for (let i = index + 1; i < operationsHistory.length; i++) {
        applyActionToString(operationsHistory[i].action);
        operationsHistory[i].updatedString = localString;
        console.log(`operation ${JSON.stringify(operationsHistory[i].action)}, ${operationsHistory[i].timestamp}, string: ${operationsHistory[i].updatedString}`)
    }
    console.log(`Client ${id} ended merging with string ${localString}, on timestamp <ending timestamp>`) // TODO: verify what is ending timestamp
}

function getPosition(string, subString, index) {
    return string.split(subString, index).join(subString).length;
}

const handleData = (socket, buffer) => {
    if (buffer.toString().substring(0, 7) === "goodbye") {
        updateGoodbyeCounter();
    }
    else {
        const parsedMessage = JSON.parse(buffer.toString().substring(0, getPosition(buffer.toString(), "}", 2) + 1));
        const { action, timestamp, cid } = parsedMessage;
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

let serversSockets = [];
let clientsSockets = [];

let server = net.createServer()
    .listen(port, "127.0.0.1", () => {
    })
    .on('connection', socket => {
        clientsSockets.push(socket);

        socket.on('data', buffer => {
            handleData(socket, buffer);
        })
    });

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

setTimeout(() => {
    eventLoop()
}, 10000)

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
        let buf = { action, timestamp: myTimestamp, cid: id };
        lastUpdates.push(buf);

        if(SPECIAL_RUN_MODE) {
            if (numOfLocalUpdates == 10) {
                lastUpdates.forEach((update) => {
                    setTimeout(() => serversSockets.forEach(serverSocket => serverSocket.write(JSON.stringify(update))), Math.random() * 3000);
                    setTimeout(() => clientsSockets.forEach(clientSocket => clientSocket.write(JSON.stringify(update))), Math.random() * 3000);
                });
                numOfLocalUpdates = 0;
                lastUpdates = [];
            } else if (numOfActions === 0) {
                lastUpdates.forEach((update) => {
                    setTimeout(() => serversSockets.forEach(serverSocket => serverSocket.write(JSON.stringify(update))), Math.random() * 3000);
                    setTimeout(() => clientsSockets.forEach(clientSocket => clientSocket.write(JSON.stringify(update))), Math.random() * 3000);
                });
            }
        } else {
            setTimeout(() => serversSockets.forEach(serverSocket => serverSocket.write(JSON.stringify(buf))), Math.random() * 3000);
            setTimeout(() => clientsSockets.forEach(clientSocket => clientSocket.write(JSON.stringify(buf))), Math.random() * 3000);
        }

    }

    for (const action of myActions) {
        setTimeout(() => sendMessage(action), Math.random() * 5000);
    }

}

setTimeout(() => {
    updateGoodbyeCounter();
    serversSockets.forEach(serverSocket => serverSocket.write(`goodbye ${id}`));
    clientsSockets.forEach(clientSocket => clientSocket.write(`goodbye ${id}`));
}, 20000);
