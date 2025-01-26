import Form from "react-bootstrap/Form";
import Dropdown from "react-bootstrap/Dropdown";

function RangeExample({
	value,
	setValue,
}: { value: number; setValue: (value: number) => void }) {
	return (
		<div className="px-3 py-2">
			<Form.Label style={{ display: "block", marginBottom: "8px" }}>
				<strong>Refresh Interval:</strong> {value} ms
			</Form.Label>
			<Form.Range
				min={100}
				max={60000}
				step={100}
				onChange={(e) => setValue(parseInt(e.target.value))}
				value={value}
				style={{ marginTop: "10px" }}
			/>
		</div>
	);
}

function DropdownMenu({
	value,
	setValue,
}: { value: number; setValue: (value: number) => void }) {
	const handleMenuClick = (e: React.MouseEvent) => {
		// Prevent the dropdown menu from closing
		e.stopPropagation();
	};

	return (
		<Dropdown align="end">
			<Dropdown.Toggle variant="success" id="dropdown-basic">
				Settings
			</Dropdown.Toggle>

			<Dropdown.Menu style={{ minWidth: "300px", padding: "10px" }}>
				<div
					onClick={handleMenuClick}
					style={{ cursor: "default", padding: "0" }}
				>
					<RangeExample value={value} setValue={setValue} />
				</div>
			</Dropdown.Menu>
		</Dropdown>
	);
}

export default DropdownMenu;
