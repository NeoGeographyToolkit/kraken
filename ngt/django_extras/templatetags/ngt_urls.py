from django import template
print "Loading ngt_urls template tag library."
register = template.Library()

class MediaURLPrefixNode(template.Node):
    def render(self, context):
        try:
            from django.conf import settings 
            return settings.MEDIA_URL
        except:
            return ''


def media_url_prefix(parser, token):
    """
    {% media_url_prefix %}
    """
    return MediaURLPrefixNode()

#template.register_tag('media_url_prefix', media_url_prefix)
register.tag('media_url_prefix', media_url_prefix)