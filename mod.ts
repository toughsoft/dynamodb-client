import {
  BatchWriteItemCommand,
  type BatchWriteItemCommandOutput,
  DynamoDBClient,
  GetItemCommand,
  type GetItemCommandOutput,
  PutItemCommand,
  type PutItemCommandOutput,
  QueryCommand,
  type QueryCommandInput,
  type QueryCommandOutput,
} from "npm:@aws-sdk/client-dynamodb@3";
import {
  marshall,
  type NativeAttributeValue,
  unmarshall,
} from "npm:@aws-sdk/util-dynamodb@3";
import * as log from "https://deno.land/std@0.218.2/log/mod.ts";

export const LoggerId = "@toughsoft/dynamodb-client";

function logger() {
  return log.getLogger(LoggerId);
}

type Modify<T, R> = Omit<T, keyof R> & R;

export type DynamoObject = { [key: string]: NativeAttributeValue };

export interface DynamodbClientOptions {
  region?: string;
  accessKey?: string;
  secretKey?: string;
}

export type GetResult = Modify<GetItemCommandOutput, {
  Item: DynamoObject;
}>;

export type PutResult = PutItemCommandOutput;

export type PutBatchResult = BatchWriteItemCommandOutput;

export type DeleteBatchResult = BatchWriteItemCommandOutput;

type CondSpec = string;

export interface KeyConditionSpec {
  partition?: {
    key: string;
    value: string | number;
  };
  sort?: {
    key: string;
    value: string | number;
    cond: CondSpec;
  };
}

export interface QueryOptions {
  keyCondition?: KeyConditionSpec;
  limit?: number;
  scanForward?: boolean;
  startKey?: DynamoObject;
  index?: string;
}

export type QueryResult = Modify<QueryCommandOutput, {
  Items: DynamoObject[];
  LastEvaluatedKey?: DynamoObject;
}>;

export interface DynamodbClient {
  get(table: string, keySpec: DynamoObject): Promise<GetResult>;
  put(table: string, item: DynamoObject): Promise<PutResult>;
  putBatch(table: string, items: DynamoObject[]): Promise<PutBatchResult>;
  deleteBatch(table: string, ids: DynamoObject[]): Promise<DeleteBatchResult>;
  query(table: string, options: QueryOptions): Promise<QueryResult>;
}

const PartitionSubs = {
  KEY: "#partitionKey",
  VALUE: ":partitionValue",
};

const SortSubs = {
  KEY: "#sortKey",
  VALUE: ":sortValue",
};

const create = (
  { region, accessKey, secretKey }: DynamodbClientOptions = {},
): DynamodbClient => {
  const clientOptions = {};

  if (region) {
    Object.assign(clientOptions, {
      region,
    });
  }

  if (accessKey || secretKey) {
    Object.assign(clientOptions, {
      credentials: {
        accessKeyId: accessKey,
        secretAccessKey: secretKey,
      },
    });
  }

  const client = new DynamoDBClient(clientOptions);

  const get = async (
    table: string,
    keySpec: DynamoObject,
  ): Promise<GetResult> => {
    const result = await client.send(
      new GetItemCommand({
        TableName: table,
        Key: marshall(keySpec),
      }),
    );

    if (result.Item === undefined) {
      throw new Error(`No item found for ${keySpec}`);
    }

    return { ...result, Item: unmarshall(result.Item) };
  };

  const getSortQueryExpr = (cond: CondSpec): string => {
    if (cond === "begins_with") {
      return `begins_with(${SortSubs.KEY}, ${SortSubs.VALUE})`;
    } else {
      return `${SortSubs.KEY} ${cond} ${SortSubs.VALUE}`;
    }
  };

  const query = async (
    table: string,
    {
      keyCondition: {
        partition: partitionSpec,
        sort: sortSpec,
      } = {},
      limit,
      startKey,
      scanForward = true,
      index,
    }: QueryOptions = {},
  ): Promise<QueryResult> => {
    const params: QueryCommandInput = {
      TableName: table,
      ScanIndexForward: scanForward,
    };

    if (partitionSpec) {
      const {
        key: partitionKey,
        value: partitionValue,
      } = partitionSpec;

      let expr = `${PartitionSubs.KEY} = ${PartitionSubs.VALUE}`;
      const attrKeys = {
        [PartitionSubs.KEY]: partitionKey,
      };
      const attrValues = marshall({
        [PartitionSubs.VALUE]: partitionValue,
      });

      if (sortSpec) {
        const {
          key: sortKey,
          value: sortValue,
          cond: sortCond = ">",
        } = sortSpec;

        expr += ` AND ${getSortQueryExpr(sortCond)}`;

        attrKeys[SortSubs.KEY] = sortKey;
        Object.assign(attrValues, marshall({ [SortSubs.VALUE]: sortValue }));
      }

      params.KeyConditionExpression = expr;
      params.ExpressionAttributeNames = attrKeys;
      params.ExpressionAttributeValues = attrValues;
    }

    if (limit) {
      params.Limit = limit;
    }

    if (startKey) {
      params.ExclusiveStartKey = marshall(startKey);
    }

    if (index) {
      params.IndexName = index;
    }

    logger().debug("scan", params);

    const output = await client.send(new QueryCommand(params));

    return {
      ...output,
      Items: output.Items ? output.Items.map((x) => unmarshall(x)) : [],
      LastEvaluatedKey: output.LastEvaluatedKey
        ? unmarshall(output.LastEvaluatedKey)
        : undefined,
    };
  };

  const put = (table: string, item: DynamoObject): Promise<PutResult> => {
    const params = {
      TableName: table,
      Item: marshall(item),
    };

    logger().debug("put", params);

    return client.send(new PutItemCommand(params));
  };

  const putBatch = (
    table: string,
    items: DynamoObject[],
  ): Promise<PutBatchResult> => {
    const params = {
      RequestItems: {
        [table]: items.map((item) => {
          return {
            PutRequest: {
              Item: marshall(item),
            },
          };
        }),
      },
    };

    logger().debug("putBatch", params);

    return client.send(new BatchWriteItemCommand(params));
  };

  const deleteBatch = (
    table: string,
    ids: DynamoObject[],
  ): Promise<DeleteBatchResult> => {
    const params = {
      RequestItems: {
        [table]: ids.map((keyMap) => {
          return {
            DeleteRequest: {
              Key: marshall(keyMap),
            },
          };
        }),
      },
    };

    logger().debug("deleteBatch", params);

    return client.send(new BatchWriteItemCommand(params));
  };

  return {
    get,
    put,
    putBatch,
    deleteBatch,
    query,
  };
};

export default create;
