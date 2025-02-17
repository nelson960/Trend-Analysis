from rest_framework import serializers

class TweetProcessSerializer(serializers.Serializer):
    # Expecting raw_data to be a dict with a key like "tweets" holding a list of strings.
    raw_data = serializers.DictField(child=serializers.ListField(child=serializers.CharField()))
    column_name = serializers.CharField(default="tweets", required=False)
