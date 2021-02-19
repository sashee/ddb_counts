import {DynamoDBClient, GetItemCommand, PutItemCommand, TransactWriteItemsCommand, paginateScan, BatchWriteItemCommand} from "@aws-sdk/client-dynamodb";
import {Credentials} from "@aws-sdk/types";
import {defaultProvider} from "@aws-sdk/credential-provider-node";
import {AssumeRoleParams} from "@aws-sdk/credential-provider-ini";
import {STS} from "@aws-sdk/client-sts";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";

async function assume(sourceCreds: Credentials, params: AssumeRoleParams): Promise<Credentials> {
	const sts = new STS({credentials: sourceCreds});
	const result = await sts.assumeRole(params);
	if(!result.Credentials) {
		throw new Error("unable to assume credentials - empty credential object");
	}
	return {
		accessKeyId: String(result.Credentials.AccessKeyId),
		secretAccessKey: String(result.Credentials.SecretAccessKey),
		sessionToken: result.Credentials.SessionToken
	}
}

const client = new DynamoDBClient({credentials: defaultProvider({roleAssumer: assume})});

const outputs = JSON.parse(process.env.TERRAFORM_OUTPUT);

const USERS_TABLE: string = outputs["users-table"].value;
const COUNTS_TABLE: string = outputs["counts-table"].value;

const clearDbs = async () => {
	for await (const page of paginateScan({client, pageSize: 25}, {TableName: USERS_TABLE, ProjectionExpression: "#pk", ExpressionAttributeNames: {"#pk": "ID"}})) {
		if (page.Items?.length > 0) {
			await client.send(new BatchWriteItemCommand({
				RequestItems: {
					[USERS_TABLE]: page.Items.map((item) => ({
						DeleteRequest: {
							Key: item
						},
					})),
				},
			}));
		}
	}
	for await (const page of paginateScan({client, pageSize: 25}, {TableName: COUNTS_TABLE, ProjectionExpression: "#pk", ExpressionAttributeNames: {"#pk": "type"}})) {
		if (page.Items?.length > 0) {
			await client.send(new BatchWriteItemCommand({
				RequestItems: {
					[COUNTS_TABLE]: page.Items.map((item) => ({
						DeleteRequest: {
							Key: item
						},
					})),
				},
			}));
		}
	}
};

const getCount = async () => {
	const res = await client.send(new GetItemCommand({
		TableName: COUNTS_TABLE,
		Key: marshall({
			type: "users",
		}),
	}));
	return unmarshall(res.Item).count;
}

type User = {
	ID: string,
	name: string,
};

const addUser = async (item: User) => {
	await client.send(new TransactWriteItemsCommand({
		TransactItems: [
			{
				Put: {
					TableName: USERS_TABLE,
					ConditionExpression: "attribute_not_exists(#pk)",
					ExpressionAttributeNames: {"#pk": "ID"},
					Item: marshall(item),
				}
			},
			{
				Update: {
					TableName: COUNTS_TABLE,
					UpdateExpression: "ADD #count :count",
					ExpressionAttributeNames: {"#count": "count"},
					ExpressionAttributeValues: marshall({":count": 1}),
					Key: marshall({type: "users"}),
				}
			}
		]
	}));
}

const removeUser = async (ID: string) => {
	await client.send(new TransactWriteItemsCommand({
		TransactItems: [
			{
				Delete: {
					TableName: USERS_TABLE,
					ConditionExpression: "attribute_exists(#pk)",
					ExpressionAttributeNames: {"#pk": "ID"},
					Key: marshall({ID}),
				}
			},
			{
				Update: {
					TableName: COUNTS_TABLE,
					UpdateExpression: "ADD #count :count",
					ExpressionAttributeNames: {"#count": "count"},
					ExpressionAttributeValues: marshall({":count": -1}),
					Key: marshall({type: "users"}),
				}
			}
		]
	}));
}

const modifyUser = async (item: User) => {
	await client.send(new PutItemCommand({
		TableName: USERS_TABLE,
		ConditionExpression: "attribute_exists(#pk)",
		ExpressionAttributeNames: {"#pk": "ID"},
		Item: marshall(item),
	}));
};

(async () => {
	await clearDbs();
	const item = await client.send(new GetItemCommand({TableName: COUNTS_TABLE, Key: marshall({type: "users"})}));
	if (!item.Item) {
		// initialize count to 0
		await client.send(new PutItemCommand({
			TableName: COUNTS_TABLE,
			Item: marshall({type: "users", count: 0}),
			ConditionExpression: "attribute_not_exists(#pk)",
			ExpressionAttributeNames: {"#pk": "type"},
		}));
	}

	console.log("count: " + await getCount());
	console.log("Add user 1");
	await addUser({ID: "1", name: "user1"});
	console.log("count: " + await getCount());
	console.log("Add user 2");
	await addUser({ID: "2", name: "user2"});
	console.log("count: " + await getCount());
	try {
		await addUser({ID: "2", name: "user2"});
	}catch (e) {
		console.log("can not overwrite a user with the addUser");
		console.log(e.CancellationReasons)
	}
	console.log("Modify user 1")
	await modifyUser({ID: "1", name: "newname"});
	console.log("count: " + await getCount());
	try {
		await modifyUser({ID: "3", name: "newname"});
	}catch (e) {
		console.log("can not add a user with the modifyUser");
	}
	console.log("Remove user 2")
	await removeUser("2");
	console.log("count: " + await getCount());
	try {
		await removeUser("2");
	}catch(e) {
		console.log("can not remove a non-existing user");
		console.log(e.CancellationReasons)
	}
})();
