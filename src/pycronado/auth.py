# src/pycronado/auth.py


def requires(*param_names, permissions=None):
    # normalize permissions into a list so we can iterate uniformly
    if permissions is None:
        required_perms = None
    elif isinstance(permissions, (list, tuple, set)):
        required_perms = list(permissions)
    else:
        required_perms = [permissions]

    def decorator(method):
        def wrapper(self, *args, **kwargs):
            # --- 1. Extract/validate required params ---
            for param_name in param_names:
                value = self.param(param_name)
                if value is None:
                    return self.jsonerr(f"`{param_name}` parameter is required", 400)
                kwargs[param_name] = value

            # --- 2. Enforce permission(s), if requested ---
            if required_perms is not None:
                has_any = any(
                    getattr(self, "has_permission", lambda *_: False)(perm)
                    for perm in required_perms
                )

                if not has_any:
                    return self.jsonerr("forbidden", 403)

            # --- 3. Call the actual handler ---
            return method(self, *args, **kwargs)

        return wrapper

    return decorator


class UserMixin:
    def token(self):
        if not hasattr(self, "JWT"):
            self.JWT = self.decoded_jwt()
        return self.JWT

    def issuer(self):
        return self.decoded_jwt()["iss"]

    def user(self):
        jwt = self.decoded_jwt()
        return jwt["user"]

    def username(self):
        return self.user()["username"]

    def user_id(self):
        return f"{self.issuer()}::{self.username()}"

    def permissions(self):
        return self.user().get("permissions", [])

    def has_permission(self, ability, group=None):
        for perm in self.permissions():
            p_group = perm.get("user_group")
            p_ability = perm.get("group_ability")

            if not p_group or not p_ability:
                continue  # malformed row, ignore

            ability_ok = (p_ability == ability) or (p_ability == "*")
            group_ok = group is None or p_group == group or p_group == "*"

            if ability_ok and group_ok:
                return True

        return False
