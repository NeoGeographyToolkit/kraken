from django.db import models

# Model for an image measure
class ImageMeasure(models.Model):
    image_identifier = models.CharField(max_length=200)
    date = models.DateTimeField('date captured')
    col = models.FloatField()
    row = models.FloatField()

    def __unicode__(self):
        return self.image_identifier + " " + str(self.col) + " " + str(self.row)


# Model for a geographic measure
class GeographicMeasure(models.Model):
    basemap = models.CharField(max_length=200)
    date = models.DateTimeField('date captured')
    lon = models.FloatField()
    lat = models.FloatField()
    alt = models.FloatField()

    def __unicode__(self):
        return self.basemap + " " + str(self.lat) + " " + str(self.lon) + " " + str(self.alt)

# Model for a tiepoint
class TiePoint(models.Model):
    geo_measure = models.ForeignKey(GeographicMeasure)
    image_measure = models.ForeignKey(ImageMeasure)

    def __unicode__(self):
        return str(self.geo_measure) + " " + str(self.image_measure)


