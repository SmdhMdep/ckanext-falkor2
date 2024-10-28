import ckan.lib.base as base
from flask import Blueprint
render = base.render

falkor_blueprint = Blueprint(u'falkor_blueprint', __name__)


def admin_tab():
    return render(
        "admin/base.html",
    )


falkor_blueprint.add_url_rule(
    u'/ckan-admin/falkor', view_func=admin_tab
)
