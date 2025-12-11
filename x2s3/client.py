from dataclasses import dataclass


@dataclass
class ObjectHandle:
    """Base handle to an opened object ready for streaming.

    This contains common metadata for all storage backends.
    Client implementations should subclass this to add their
    specific content source references.
    """
    target_name: str
    key: str
    status_code: int
    headers: dict
    media_type: str
    content_length: int


class ProxyClient:
    """ Interface for a client that implements an S3-like interface
        to key-value access against some backend service.

        Note that this interface does not try to encode the entire S3 API.
        We only care about the bare-bones functionality that is required
        for viewers like Neuroglancer, N5 Viewer, Vizarr, etc.
    """

    async def head_object(self, key: str):
        """
        Basic interface for AWS S3's HeadObject API.
        https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
        """

    async def open_object(self, key: str, range_header: str = None):
        """
        Open an object and return a handle for streaming.

        This performs the file/storage operation and returns an ObjectHandle
        containing metadata and a reference to the content, or an error Response.

        Returns:
            ObjectHandle on success, or Response on error
        """

    def stream_object(self, handle: ObjectHandle):
        """
        Stream content from an opened object handle.

        Args:
            handle: An ObjectHandle returned from open_object()

        Returns:
            StreamingResponse that streams the object content
        """

    async def get_object(self, key: str, range_header: str = None):
        """
        Basic interface for AWS S3's GetObject API.
        https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html

        This is a convenience method that combines open_object() and stream_object().
        """

    async def list_objects_v2(self,
                            continuation_token: str,
                            delimiter: str,
                            encoding_type: str,
                            fetch_owner: str,
                            max_keys: str,
                            prefix: str,
                            start_after: str):
        """
        Basic interface for AWS S3's ListObjectsV2 API.
        https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
        """
    