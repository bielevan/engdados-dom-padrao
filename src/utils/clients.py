import boto3


class AWSClients:


    @staticmethod
    def get_client_dynamodb(table: str) -> boto3.client:
        """_summary_

        Args:
            table (str): _description_

        Returns:
            boto3.client: _description_
        """
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(table)
        return table