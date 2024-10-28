import ckan.lib.base as base
from flask import Blueprint
render = base.render

falkor_blueprint = Blueprint(u'falkor_blueprint', __name__)


def falkor_audit():
    return render(
        "falkor-audit.html",
    )


falkor_blueprint.add_url_rule(
    u'/ckan-admin/falkor-audit', view_func=falkor_audit
)
